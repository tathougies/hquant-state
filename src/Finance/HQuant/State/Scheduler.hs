{-# LANGUAGE RecordWildCards, NamedFieldPuns, TupleSections, DoAndIfThenElse #-}
-- | The scheduler is responsible for running aggregations as scheduled, and ensuring that we get
--   rid of data as specified in the configuration.
module Finance.HQuant.State.Scheduler (maintainSeries) where

import Finance.HQuant.State.ACID hiding (aggregate)
import Finance.HQuant.State.Config
import Finance.HQuant.State.Types
import Finance.HQuant.State.Storage

import Control.Applicative
import Control.Monad
import Control.Monad.Writer.Lazy
import Control.Concurrent
import Control.Concurrent.STM
import Control.Lens

import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.ByteString as BS
import Data.Sequence (Seq)
import Data.Function
import Data.Ord
import Data.Maybe
import Data.List
import Data.Time.Clock
import Data.Acid
import Data.ByteString (ByteString)

import System.Log.Logger

newtype Schedule     = Schedule [ScheduleItem]
data    ScheduleItem = ScheduleItem
    { schItemTimes      :: [UTCTime]
    , schItemAction     :: SchedulableAction (SeriesName, SeriesDeclaration) }

data SchedulerState = SchedulerState
    { schStSchedule   :: TVar Schedule
    , schStAcidState  :: AcidState HQuantState
    , schStPerforming :: TVar (S.Set (SchedulableAction SeriesName))
    , schStSchLastUpd :: TVar UTCTime -- ^ This is written to whenever there was a schedule change
    }

moduleName = "Finance.HQuant.State.Scheduler"

freqToDuration :: Frequency -> Duration
freqToDuration (Every x units) = For (fromIntegral x) units

timesForSchedule :: UTCTime -> Frequency -> [UTCTime]
timesForSchedule s f = let period = freqToDuration f
                       in iterate (addDuration period) s

getSoonestScheduledTime :: [ScheduleItem] -> Maybe UTCTime
getSoonestScheduledTime items = foldr minTimes Nothing items
    where minTimes (ScheduleItem (time:_) _) Nothing = Just time
          minTimes (ScheduleItem (time:_) _) r@(Just t)
              | time < t  = Just time
              | time == t = Just t
              | otherwise = r

markComplete :: UTCTime -> [ScheduleItem]
             -> ([ScheduleItem], [(UTCTime, SchedulableAction (SeriesName, SeriesDeclaration))])
markComplete timeCompleted items = foldr doMarks ([], []) items
    where doMarks item@(ScheduleItem times action) (items, actions)
              -- If this action should have completed, we remove this time, and continue the action
              | head times <= timeCompleted = let (runAt, times') = break (> timeCompleted) times
                                             -- The actions will be scheduled for all times in runAt.
                                              in ( ScheduleItem times' action:items
                                                 , map (,action) runAt ++ actions)
              | otherwise                   = (item:items, actions)

-- | Halts the scheduler until the specified time, unless a new scheduled item comes in which means
--   we will need to update the scheduling table...
waitUntil :: SchedulerState -> UTCTime -> IO Bool
waitUntil st time = do
  curTime <- getCurrentTime
  if (curTime < time)
  then do
   let timeToWait = time `diffUTCTime` curTime
       timeToWaitInSeconds = ceiling ((toRational  timeToWait) * 1000000)
   waitingTVar <- registerDelay timeToWaitInSeconds
   let wait = do
         timeComplete <- readTVar waitingTVar
         -- If the time has completed, then we return True to signal that we've successfully waited
         -- until time
         if timeComplete then return True else retry
       waitOnScheduleChange = do
         scheduleLastUpdated <- readTVar (schStSchLastUpd st)
         -- If the schedule has been updated since we last saw it, we return False to indicate that
         -- we need to recheck the schedule
         if scheduleLastUpdated >= curTime then return False else retry
   atomically (wait `orElse` waitOnScheduleChange)
  else return True -- If we've already reached the new time, then return True to signal that we
                   -- should continue with whatever actions need to be run

storeTimeSeries :: SchedulerState -> UTCTime -> SeriesName -> SeriesDeclaration -> IO () -> IO ()
storeTimeSeries st at sn d signalFinished = do
  let Just storageService = d ^. sdStorageService
  r <- query (schStAcidState st) (TimeSeriesQuery sn (addDuration (negateDuration (freqToDuration (storageService ^. ssdFrequency))) at) at)
  case r of
    Left err -> do
           warningM moduleName $ "storeTimeSeries: " ++ err
           signalFinished
    Right (times, r)  -> do
           let columns = map unWrapSeries r
               columnNames = map _fieldDName (_schemaDFields . _tsdSchema $ d)
           forkIO (store d at sn (formatSeries columnNames times columns))
           signalFinished

cullTimeSeries :: SchedulerState -> UTCTime -> SeriesName -> SeriesDeclaration -> IO () -> IO ()
cullTimeSeries st at sn d signalFinished = do
  -- update (schStAcidState st) (DeleteData sn at ((_tsdKeepFor d)`addDuration` at))
  infoM moduleName $ concat [ "Cull time series '", T.unpack . unSeriesName $ sn, "'. Delete from "
                            , show at, " to ", show ((_tsdKeepFor d) `addDuration` at)]
  signalFinished

aggregate :: SchedulerState -> UTCTime -> AggregationName -> SeriesName -> SeriesDeclaration
          -> IO () -> IO ()
aggregate st at an sn d signalFinished = do
  infoM moduleName $ concat ["aggregating ", (T.unpack . unSeriesName $ an), " for "
                            , T.unpack . unSeriesName $ sn]
  let Just ss = _aggStorageService d
  r <- query (schStAcidState st) (Aggregate an sn ((negateDuration . freqToDuration $ ss ^. ssdFrequency) `addDuration` at) at)
  case r of
    Left err         -> do
           warningM moduleName $ "aggregate: " ++ err
           signalFinished
    Right (times, r) -> do
           let columns = map unWrapSeries r
               columnNames = map _aggFieldName (_aggSchemaFields . _aggSchema $ d)
           forkIO (store d at sn (formatSeries columnNames times columns))
           signalFinished

getLastRunTime :: SchedulerState -> Frequency -> SchedulableAction SeriesName -> IO UTCTime
getLastRunTime st freq act = do
  lastRun <- query (schStAcidState st) (WhenLastRun act)
  case lastRun of
    Right x -> return x -- This was scheduled before, so we can just use this last run time as a
                        -- basis.
    Left  _ -> do
      now <- getCurrentTime
      let lastTime = roundUp now (freqToDuration freq)
      update (schStAcidState st) (UpdateLastRunTime act lastTime)
      -- Now we need to calculate a base run time by taking the current time and rounding it up to
      -- the nearest multiple of freq.
      return lastTime

instantiateSchedulingAction :: SchedulerState -> Frequency ->
                               SchedulableAction (SeriesName, SeriesDeclaration) -> IO ()
instantiateSchedulingAction st freq act = do
  -- We want to attempt to see if we already are performing this action
  let genericAction = fmap fst act
  alreadyPerforming <- atomically (S.member genericAction <$> readTVar (schStPerforming st))
  when (not alreadyPerforming) $ do
    -- We're not already performing this action, so we need to check the acidstate to see if we've
    -- ever run this before.
    infoM moduleName $ "Scheduling " ++ show genericAction ++ " " ++ show freq
    lastRunTime <- getLastRunTime st freq genericAction
    let times   = iterate (addDuration (freqToDuration freq)) lastRunTime
        newItem = ScheduleItem times act
    now <- getCurrentTime
    atomically $ do
      modifyTVar (schStPerforming st) (S.insert genericAction)
      modifyTVar (schStSchedule st)   (\(Schedule items) -> Schedule (newItem:items))
      writeTVar  (schStSchLastUpd st) now

actionsFor :: SchedulerState -> SeriesDeclaration -> SeriesName
           -> [(Frequency, SchedulableAction (SeriesName, SeriesDeclaration))]
actionsFor st d@(TimeSeriesDecl {}) sn = execWriter $ do
  case _tsdStorageService d of
    Nothing -> return Nothing
    Just sd -> do
      tell [(sd ^. ssdFrequency, StoreTimeSeries (sn, d))]
      return (Just (sd ^. ssdFrequency))
  case _tsdKeepFor d of
    Forever     -> return Nothing
    For x units -> do
      tell [(Every (fromIntegral x) units, CullTimeSeries (sn, d))]
      return (Just (Every (fromIntegral x) units))
actionsFor st d@(AggregationDecl {}) sn = [(_aggFrequency d, PerformAggregation (_aggName d, d) (sn, d))]

updateSchedule :: SchedulerState -> SeriesDeclaration -> IO ()
updateSchedule st d = do
  r <- query (schStAcidState st) ListTimeSeries
  let lookingFor = case d of
                     TimeSeriesDecl  {} -> _tsdNamePattern d
                     AggregationDecl {} -> _aggTarget d
      applicable = filter (`matchesPattern` lookingFor) r
  mapM_ (mapM_ (uncurry (instantiateSchedulingAction st)) . actionsFor st d) applicable

runAction :: SchedulerState -> UTCTime -> SchedulableAction (SeriesName, SeriesDeclaration) -> IO ()
runAction st at action = runAction'
    where runAction' = case action of
                         StoreTimeSeries    (sn, d)  -> storeTimeSeries st at sn d finishAction
                         CullTimeSeries     (sn, d)  -> cullTimeSeries  st at sn d finishAction
                         PerformAggregation (an, d) (sn, _) -> aggregate st at an sn d finishAction

          finishAction = update (schStAcidState st) (UpdateLastRunTime genericAction at)
          genericAction = fmap fst action

-- | This will properly determine the order of the actions and run them.
runActions :: SchedulerState
           -> [(UTCTime, SchedulableAction (SeriesName, SeriesDeclaration))]
           -> IO ()
runActions st actions = do
  let actionSeries (StoreTimeSeries    sn)   = fst sn
      actionSeries (CullTimeSeries     sn)   = fst sn
      actionSeries (PerformAggregation _ sn) = fst sn

      grouped = groupBy ((==) `on` (actionSeries . snd)) $
                sortBy (comparing (actionSeries . snd)) actions

      -- This function produces the relative order of all actions. This makes sure culls always go
      -- last.
      relOrder (time, StoreTimeSeries _)      = (time, 0)
      relOrder (time, CullTimeSeries _)       = (time, 3)
      relOrder (time, PerformAggregation _ _) = (time, 0)
      groupedAndSorted = map (sortBy (comparing relOrder)) grouped -- This ensures that all grouped
                                                                  -- actions are run in order by time
  mapM_ (forkIO . mapM_ (uncurry (runAction st))) groupedAndSorted -- This will run all the actions in separate threads...

runSchedule :: SchedulerState -> IO ()
runSchedule st = runSchedule'
    where runSchedule' = do
            nextStatus <- atomically $ do
              Schedule items <- readTVar (schStSchedule st)
              case getSoonestScheduledTime items of
                Nothing       -> return Nothing
                Just nextTime -> do
                    return (Just nextTime)
            case nextStatus of
              Nothing       -> do
                              now <- getCurrentTime
                              atomically $ do
                                 lastUpdated <- readTVar (schStSchLastUpd st)
                                 if lastUpdated >= now then return () else retry
                              runSchedule'
              Just nextTime -> do
                  now <- getCurrentTime
                  infoM moduleName $ concat [show now, ": runSchedule: waiting until ", show nextTime]
                  performActions <- waitUntil st nextTime
                  when performActions $ do
                    infoM moduleName $ concat ["runSchedule: performing actions at ", show nextTime]
                    -- We've waited until the specified time, now go ahead
                    nextItems <- atomically $ do
                        Schedule items <- readTVar (schStSchedule st)
                        let (items', nextItems) = markComplete nextTime items
                        writeTVar (schStSchedule st) (Schedule items')
                        return nextItems
                    runActions st nextItems
                  -- If we haven't waited enough time, then we just recurse with the knowledge that
                  -- if any schedule changes happened that would have affected what should run next,
                  -- those actions will be run.
                  runSchedule'

updateSchedules :: SchedulerState -> [SeriesDeclaration] -> IO ()
updateSchedules st seriess = forever $ do
    mapM_ (updateSchedule st) seriess -- This will instantiate all the
                                      -- events we need
    threadDelay 30000000

maintainSeries :: AcidState HQuantState -> [SeriesDeclaration] -> IO ()
maintainSeries acidSt seriess = do
  scheduleV    <- newTVarIO (Schedule []) -- We start with the empty schedule
  performingV  <- newTVarIO S.empty     -- We start performing nothing
  now          <- getCurrentTime
  lastUpdatedV <- newTVarIO now
  let st = SchedulerState
           { schStSchedule   = scheduleV
           , schStAcidState  = acidSt
           , schStPerforming = performingV
           , schStSchLastUpd = lastUpdatedV }
      seriesName d@(AggregationDecl {}) = SeriesName . unTSNamePattern . _aggTarget $ d
      seriesName d@(TimeSeriesDecl {})  = SeriesName . unTSNamePattern . _tsdNamePattern $ d

  -- This will set a default schedule update for us
  forkIO (updateSchedules st seriess)
  runSchedule st -- This will continuously run the schedule