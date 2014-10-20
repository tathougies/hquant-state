{-# LANGUAGE GeneralizedNewtypeDeriving, TemplateHaskell, RankNTypes, DeriveDataTypeable, StandaloneDeriving, DeriveFoldable, DeriveFunctor #-}
module Finance.HQuant.State.Types where

import Prelude hiding (foldr)

import Control.Monad.Identity
import Control.Concurrent.MVar
import Control.Comonad
import Control.Applicative

import qualified Data.Text as T
import Data.String
import Data.SafeCopy
import Data.Serialize
import Data.Acid
import Data.Time.Clock
import Data.Monoid
import Data.Sequence (Seq)
import Data.Typeable
import Data.ByteString (ByteString)
import Data.Int
import Data.Time.Calendar
import Data.Foldable hiding (concat)
import Data.Functor
import Data.Ratio

import Network.HTTP.Conduit (Manager)

newtype SeriesName = SeriesName { unSeriesName :: T.Text }
    deriving (Show, Read, Eq, Ord, IsString, SafeCopy, Typeable)
newtype FieldName = FieldName { unFieldName :: T.Text }
    deriving (Show, Read, Eq, Ord, IsString, SafeCopy)
newtype TSNamePattern = TSNamePattern { unTSNamePattern :: T.Text }
    deriving (Show, Read, Eq, Ord, IsString, SafeCopy)
type AggregationName = SeriesName

{-# INLINE matchesPattern #-}
matchesPattern :: SeriesName -> TSNamePattern -> Bool
matchesPattern (SeriesName name) (TSNamePattern pattern) = pattern `T.isPrefixOf` name

-- ** Series types

data SeriesSeq f = FixedSeq   Int16 (f Int64)
                 | StringSeq  (f ByteString)
                 | IntegerSeq (f Int64)

newtype DoubleSeq a = DoubleSeq { unDoubleSeq :: Seq (Seq a) }
newtype MaybeSeq a = MaybeSeq { unMaybeSeq :: Seq (Maybe a) }
newtype UnfoldTuple f a = UnfoldTuple { unUnfoldTuple :: (f a, Seq (f a)) }

newtype SeriesSeqWrapper = WrapSeries { unWrapSeries :: SeriesSeq Seq }
    deriving Typeable

data Datum       = FixedD Int64
                   -- ^ This just contains the digits, the precision is implicit...
                 | IntegerD Int64
                 | StringD ByteString
                   deriving (Typeable)

data QualifiedDatum = FixedQD   Rational
                    | IntegerQD Int64
                    | StringQD  ByteString
                      deriving (Typeable)

data SchedulableAction sn = StoreTimeSeries    sn
                          | PerformAggregation sn sn
                          | CullTimeSeries     sn
                            deriving (Show, Eq, Ord, Functor, Typeable)

-- *** Operations

-- adjustSeq :: (forall a. Seq a -> Seq a) -> SeriesSeq -> SeriesSeq
-- adjustSeq f = fst . adjustSeq' (\s -> (f s, ()))

-- adjustSeq' :: (forall a. Seq a -> (Seq a, b)) -> SeriesSeq -> (SeriesSeq, b)
-- adjustSeq' f (FixedSeq e s) = let (d, b) = f s
--                               in (FixedSeq e d, b)
-- adjustSeq' f (StringSeq s) = let (d, b) = f s
--                              in (StringSeq d, b)
-- adjustSeq' f (IntegerSeq s) = let (d, b) = f s
--                               in (IntegerSeq d, b)

-- adjustSeqPure :: (Comonad f, Applicative g) => (forall a. Ord a => Seq a -> Seq a) -> SeriesSeq f -> SeriesSeq g
-- adjustSeqPure f s = adjustSeq (pure . f . extract) s

adjustSeq :: (forall a. Ord a => f a -> g a) -> SeriesSeq f -> SeriesSeq g
adjustSeq f (FixedSeq e s) = FixedSeq e (f s)
adjustSeq f (StringSeq  s) = StringSeq (f s)
adjustSeq f (IntegerSeq s) = IntegerSeq (f s)

adjustSeq2 :: (forall a. Ord a => f a -> e a -> g a) -> SeriesSeq f -> SeriesSeq e -> SeriesSeq g
adjustSeq2 f (FixedSeq e s) (FixedSeq e' s')
    | e == e' = FixedSeq e (f s s')
adjustSeq2 f (StringSeq s) (StringSeq s') = StringSeq (f s s')
adjustSeq2 f (IntegerSeq s) (IntegerSeq s') = IntegerSeq (f s s')
adjustSeq2 _ _ _ = error "Type mismatch in adjustSeq2"

adjustSeq' :: (forall a. (Show a, Ord a) => f a -> b) -> SeriesSeq f -> b
adjustSeq' f (FixedSeq _ s) = f s
adjustSeq' f (StringSeq  s) = f s
adjustSeq' f (IntegerSeq s) = f s

adjustSeqNum :: (forall a. Num a => f a -> g a) -> SeriesSeq f -> SeriesSeq g
adjustSeqNum f (FixedSeq e s) = FixedSeq e (f s)
adjustSeqNum f (StringSeq  s) = error "StringSeq doesn't support numeric operations"
adjustSeqNum f (IntegerSeq s) = IntegerSeq (f s)

extractSeq :: Comonad f => SeriesSeq f -> QualifiedDatum
extractSeq (FixedSeq e s) = FixedQD   (fromRational ((fromIntegral . extract $ s) % (fromIntegral e)))
extractSeq (StringSeq  s) = StringQD  (extract s)
extractSeq (IntegerSeq s) = IntegerQD (extract s)

extractSeqUnqualified :: Comonad f => SeriesSeq f -> Datum
extractSeqUnqualified (FixedSeq _ s) = FixedD (extract s)
extractSeqUnqualified (StringSeq  s) = StringD (extract s)
extractSeqUnqualified (IntegerSeq s) = IntegerD (extract s)

-- ** Time types

data TimeUnit = Years | Months | Weeks | Days | Hours | Minutes | Seconds
              deriving (Eq, Ord, Show, Read)

data Duration = For Integer TimeUnit
              | Forever
              deriving (Show, Eq, Ord)
data Frequency = Every Int TimeUnit
              deriving (Show, Eq, Ord)

-- ** Aggregation Types

data AggMethod = Last | First | Min | Max | Sum | Count
                 deriving (Show, Read, Enum, Eq, Ord, Bounded)
data AggMissingMethod fn = AMMUse fn | AMMLag fn Int | AMMIgnore
                      deriving (Show, Read, Eq, Ord)

deriveSafeCopy 0 'base ''TimeUnit
deriveSafeCopy 0 'base ''Duration
deriveSafeCopy 0 'base ''Frequency
deriveSafeCopy 0 'base ''AggMethod
deriveSafeCopy 0 'base ''AggMissingMethod
deriveSafeCopy 0 'base ''Datum
deriveSafeCopy 0 'base ''SchedulableAction

instance (Foldable m, MonadPlus m) => SafeCopy (SeriesSeq m) where
    putCopy (FixedSeq e s) = contain $ do
                               putWord8 0
                               put e
                               put (toList s)
    putCopy (IntegerSeq s) = contain $ do
                               putWord8 1
                               put (toList s)
    putCopy (StringSeq  s) = contain $ do
                               putWord8 2
                               put (toList s)
    getCopy = contain $ do
                t <- getWord8
                case t of
                  0 -> FixedSeq <$> get <*> (foldr mplus mzero . map return <$> get)
                  1 -> IntegerSeq <$> (foldr mplus mzero . map return <$> get)
                  2 -> StringSeq <$> (foldr mplus mzero . map return <$> get)
                  _ -> fail $ "Unknown tag in deserializing SeriesSeq"

instance Foldable m => Show (SeriesSeq m) where
    show (FixedSeq e s) = concat ["FixedSeq ", show e, " ", show (toList s)]
    show (IntegerSeq s) = concat ["IntegerSeq ", show (toList s)]
    show (StringSeq  s) = concat ["StringSeq ", show (toList s)]

deriving instance Show SeriesSeqWrapper
deriveSafeCopy 0 'base ''SeriesSeqWrapper

aggregationApplies :: TSNamePattern -> SeriesName -> Bool
aggregationApplies aggTarget seriesName = seriesName `matchesPattern` aggTarget

negateDuration :: Duration -> Duration
negateDuration Forever = Forever
negateDuration (For x units) = For (-x) units

addDuration :: Duration -> UTCTime -> UTCTime
addDuration (For x Years)   (UTCTime day time) = UTCTime (addGregorianYearsClip (fromIntegral x) day) time
addDuration (For x Months)  (UTCTime day time) = UTCTime (addGregorianMonthsClip (fromIntegral x) day) time
addDuration (For x Weeks)   t                  = addDuration (For (fromIntegral $ x * 7) Days) t
addDuration (For x Days)    (UTCTime day time) = UTCTime (addDays (fromIntegral x) day) time
addDuration (For x Hours)   t                  = (fromIntegral $ x * 3600) `addUTCTime` t
addDuration (For x Minutes) t                  = (fromIntegral $ x * 60) `addUTCTime` t
addDuration (For x Seconds) t                  = (fromIntegral x) `addUTCTime` t
addDuration Forever         t                  = error "Can't add Forever"

dayBase :: Day
dayBase = read "0000-01-01"

timeBase :: UTCTime
timeBase = UTCTime dayBase 0

diffMonths :: Day -> Day -> Integer
diffMonths day base = let (dayYear , dayMonth , _) = toGregorian day
                          (baseYear, baseMonth, _) = toGregorian base
                      in (dayYear - baseYear) * 12 + fromIntegral (dayMonth - baseMonth)

-- | Rounds a given date down. This assumes that dates start at January 1, 0000 00:00:00 AM
--
--   So for example Feburary 1, 0 rounded down to 5 months gives January 1, 0. February 1, 1 gives November 1, 0
roundDown :: UTCTime -> Duration -> UTCTime
roundDown (UTCTime day _) (For x Years)   = let (year, _, _) = toGregorian day
                                            in addDuration (For (- (year `mod` x)) Years) (UTCTime (fromGregorian year 1 1) 0)
roundDown (UTCTime day _) (For x Months)  = let (year, month, _) = toGregorian day
                                                months = (fromGregorian year month 1) `diffMonths` dayBase
                                            in addDuration (For (months - (months `mod` x)) Months) timeBase
roundDown t               (For x Weeks)   = roundDown t (For (fromIntegral $ x * 7) Days)
roundDown (UTCTime day _) (For x Days)    = let daysElapsed = day `diffDays` dayBase
                                            in addDuration (For (fromIntegral $ daysElapsed - (daysElapsed `mod` x)) Days) timeBase
roundDown t               (For x Hours)   = roundDown t (For (fromIntegral $ x * 3600) Seconds)
roundDown t               (For x Minutes) = roundDown t (For (fromIntegral $ x * 60)   Seconds)
roundDown t               (For x Seconds) = let diffInSeconds = floor . toRational $ t `diffUTCTime` timeBase
                                            in addDuration (For (fromIntegral $ diffInSeconds - (diffInSeconds `mod` x)) Seconds) timeBase

roundUp :: UTCTime -> Duration -> UTCTime
roundUp t d@(For x Years)   = roundDown (addDuration (For (-1) Seconds) (addDuration d t)) d
roundUp t d@(For x Months)  = roundDown (addDuration (For (-1) Seconds) (addDuration d t)) d
roundUp t d@(For x Weeks)   = roundUp t (For (fromIntegral $ x * 7) Days)
roundUp t d@(For x Days)    = roundDown (addDuration (For (-1) Seconds) (addDuration d t)) d
roundUp t d@(For x Hours)   = roundDown (addDuration (For (-1) Seconds) (addDuration d t)) d
roundUp t d@(For x Minutes) = roundDown (addDuration (For (-1) Seconds) (addDuration d t)) d
roundUp t d@(For x Seconds) = roundDown (addDuration (For (x - 1) Seconds) t) d
