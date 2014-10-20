{-# LANGUAGE TemplateHaskell, DeriveDataTypeable, TypeFamilies, RankNTypes, OverloadedStrings, NamedFieldPuns, ViewPatterns, ParallelListComp #-}
module Finance.HQuant.State.Aggregations where

import Prelude hiding (minimum, maximum, any, foldr, foldl, sum)

import Finance.HQuant.State.Types
import Finance.HQuant.State.Config

import qualified Control.Exception as E
import Control.Applicative
import Control.Lens hiding ((|>))
import Control.Comonad
import Control.Monad.State
import Control.Monad.Identity
import Control.Monad.Reader

import qualified Data.Sequence as Seq
import qualified Data.Array as A
import qualified Data.Map as M
import qualified Data.Text as T
import Data.Acid
import Data.Sequence (Seq, (|>), viewl, viewr, ViewL(..), ViewR(..), (><))
import Data.Map (Map)
import Data.Array (Array, listArray, (!))
import Data.ByteString (ByteString)
import Data.Int
import Data.Typeable
import Data.List (elemIndex)
import Data.Time.Clock
import Data.SafeCopy
import Data.Maybe
import Data.Foldable

import Debug.Trace

aggSeriesName :: AggregationName -> SeriesName -> SeriesName
aggSeriesName aggName seriesName = SeriesName (T.concat [unSeriesName aggName, ":", unSeriesName seriesName])

aggAndSeriesFromName :: SeriesName -> Maybe (AggregationName, SeriesName)
aggAndSeriesFromName aggName = case T.split (== ':') (unSeriesName aggName) of
                                 [_]    -> Just ("", aggName) -- This didn't have an aggregation prefix
                                 [a, x] -> Just (SeriesName a, SeriesName x)
                                 _      -> Nothing

extractSeriesNameFromAggName aggName = case aggAndSeriesFromName aggName of
                                         Nothing     -> error "extractSeriesNameFromAggName: Series names should not have ':'s"
                                         Just (_, a) -> a

combineTimes :: Ord a => Seq a -> Seq a -> Seq a
combineTimes (viewl -> EmptyL) _ = Seq.empty
combineTimes _ (viewl -> EmptyL) = Seq.empty
combineTimes xx@(viewl -> (x :< xs)) yy@(viewl -> (y :< ys))
    | x == y = x <| combineTimes xs ys
    | x < y  = combineTimes xs yy
    | x > y  = combineTimes xx ys

selectTimes :: Ord a => Seq a -> Seq a -> SeriesSeq Seq -> SeriesSeq Seq
selectTimes selected actual = adjustSeq (selectTimes' selected actual)
    where selectTimes' (viewl -> EmptyL) _ _ = Seq.empty
          selectTimes' _ (viewl -> EmptyL) _ = Seq.empty
          selectTimes' xx@(viewl -> (x :< xs)) yy@(viewl -> (y :< ys)) allS@(viewl -> (s :< ss))
              | x == y = s <| selectTimes' xs ys ss
              | x < y  = selectTimes' xs yy allS
              | x > y  = selectTimes' xx ys allS

aToAAggregation :: AggMethod -> Bool
aToAAggregation Last  = True
aToAAggregation First = True
aToAAggregation Min   = True
aToAAggregation Max   = True
aToAAggregation _     = False

aggregateAToA :: Ord a => AggMethod -> Seq (Seq a) -> Seq (Maybe a)
aggregateAToA Last seriess = fmap getLast seriess
    where getLast (viewr -> EmptyR) = Nothing
          getLast (viewr -> _ :> r) = Just r
aggregateAToA First seriess = fmap getFirst seriess
    where getFirst (viewl -> EmptyL) = Nothing
          getFirst (viewl -> l :< _) = Just l
aggregateAToA Min seriess   = fmap getMin seriess
    where getMin s
              | Seq.null s = Nothing
              | otherwise  = Just (minimum s)
aggregateAToA Max seriess   = fmap getMax seriess
    where getMax s
              | Seq.null s = Nothing
              | otherwise = Just (maximum s)

-- | A complex migration is something that can change the type of the underlying series.
--   In this case we can't just use adjustSeq, we have to use adjustSeq' and handle each method
--   individually for the type checker to be happy.
runComplexMigration :: AggMethod -> SeriesSeq DoubleSeq -> SeriesSeq MaybeSeq
runComplexMigration Sum seriess = adjustSeqNum (MaybeSeq . fmap doSum . unDoubleSeq) seriess
    where doSum s
              | Seq.null s = Nothing
              | otherwise  = Just (sum s)
runComplexMigration Count seriess = adjustSeq' (IntegerSeq . MaybeSeq . fmap (Just . fromIntegral . Seq.length) . unDoubleSeq) seriess

runAggregation :: Seq UTCTime -> [(FieldName, SeriesSeq DoubleSeq)] -> AggFieldDeclaration -> Either String (SeriesSeq MaybeSeq)
runAggregation periods groupedData declaration =
    case lookup (declaration ^. aggTargetFieldName) groupedData of
      Nothing     -> Left $ "Couldn't find target field " ++ (T.unpack . unFieldName . _aggTargetFieldName $ declaration)
      Just inData
          | aToAAggregation (_aggFieldMethod declaration) -> Right $ adjustSeq (MaybeSeq . aggregateAToA (_aggFieldMethod declaration) . unDoubleSeq) inData
          | otherwise                                     -> Right $ runComplexMigration (_aggFieldMethod declaration) inData

                     --     doAggregation :: Ord a => Seq (Seq a) -> (Seq a, Seq UTCTime)
                     --     doAggregation ss = let aggregated = aggregate (_aggFieldMethod declaration) ss
                     --                            (aggregated', times') = removeNothings aggregated
                     --                        in (aggregated', times')

                     --     removeNothings :: Seq (Maybe a) -> (Seq a, Seq UTCTime)
                     --     removeNothings aggregated = let all'        = Seq.filter (isJust . snd) $ Seq.zip periods aggregated
                     --                                     times'      = fmap fst all'
                     --                                     aggregated' = fmap (fromJust . snd) all'
                     --                                 in (aggregated', times')
                     -- in (times', aggregated)

-- lookupSeries :: FieldName -> Map FieldName (SeriesSeq MaybeSeq) -> SeriesSeq MaybeSeq
-- lookupSeries name m = case M.lookup name m of
--                         Nothing -> error $ "Couldn't find " ++ (T.unpack . unFieldName $ name)
--                         Just x -> x

-- wrapMaybeSeq x = MaybeSeq . x . unMaybeSeq

-- prepareReplacements :: AggMissingMethod -> SeriesSeq MaybeSeq -> Map FieldName (SeriesSeq MaybeSeq) -> SeriesSeq MaybeSeq
-- prepareReplacements AMMIgnore series _ = adjustSeq (wrapMaybeSeq (\series -> Seq.fromList $ replicate (Seq.length series) Nothing)) series
-- prepareReplacements (AMMUse fieldName) _ seriess = lookupSeries fieldName seriess
-- prepareReplacements (AMMLag fieldName by) s seriess
--     | by == 0 = prepareReplacements (AMMUse fieldName) s seriess
--     | by <  0 = adjustSeq (wrapMaybeSeq (\series -> Seq.drop (-by) series >< Seq.fromList (replicate (max 0 (Seq.length series + by)) Nothing))) series
--     | by >  0 = adjustSeq (wrapMaybeSeq (\series -> Seq.fromList (replicate (min (Seq.length series) by) Nothing) >< Seq.take (Seq.length series - by) series)) series
--     where series = lookupSeries fieldName seriess

-- computeMissing :: AggFieldDeclaration -> Map FieldName (SeriesSeq MaybeSeq) -> Map FieldName (SeriesSeq MaybeSeq)
-- computeMissing fieldDecl fields =
--     let series = M.lookup (_aggFieldName fieldDecl) fields
--     in case series of
--          Nothing -> error $ "Couldn't find series " ++ (T.unpack . unFieldName . _aggFieldName $ fieldDecl)
--          Just series -> let replace seq replacements = fmap (\(toReplace, replacement) ->
--                                                     case toReplace of
--                                                       Nothing -> replacement
--                                                       Just x -> toReplace) $ Seq.zip seq replacements


--                             finalReplacements = prepareReplacements (_aggFieldMissingMethod fieldDecl) series fields

--                             series' = adjustSeq2 (\seq replacements -> MaybeSeq (replace (unMaybeSeq seq) (unMaybeSeq replacements))) series finalReplacements
--                         in M.insert (_aggFieldName fieldDecl) series' fields

missingMethodFieldsToIndex :: AggMissingMethod FieldName -> [FieldName] -> Maybe (AggMissingMethod Int)
missingMethodFieldsToIndex AMMIgnore _ = pure AMMIgnore
missingMethodFieldsToIndex (AMMUse fn) fns = AMMUse <$> elemIndex fn fns
missingMethodFieldsToIndex (AMMLag fn i) fns = AMMLag <$> elemIndex fn fns <*> pure i

runMissingMethod :: AggMissingMethod Int -> SeriesSeq MaybeSeq -> [SeriesSeq MaybeSeq] -> [SeriesSeq Maybe] -> SeriesSeq Maybe
runMissingMethod AMMIgnore      series _       _     = adjustSeq (const Nothing) series
runMissingMethod (AMMUse idx)   _      _       accum = accum !! idx
runMissingMethod (AMMLag idx i) _      seriess _     = let series = seriess !! idx
                                                           lag :: Seq (Maybe a) -> Maybe a
                                                           lag series = if Seq.length series >= i
                                                                        then series `Seq.index` (Seq.length series - i)
                                                                        else Nothing
                                                       in adjustSeq (lag . unMaybeSeq) series


unfoldSeriesI :: [SeriesSeq Seq] -> [[SeriesSeq Identity]]
unfoldSeriesI seriess
    | any (adjustSeq' Seq.null) seriess = []
    | otherwise = map (adjustSeq (fst . unUnfoldTuple)) headsAndTails : rest
    where headsAndTails = map (adjustSeq doUnfold) seriess

          rest = if any (adjustSeq' (Seq.null . snd . unUnfoldTuple)) headsAndTails
                 then []
                 else unfoldSeriesI (map (adjustSeq (fmap extract . snd . unUnfoldTuple)) headsAndTails)

          doUnfold :: Seq a -> UnfoldTuple Identity a
          doUnfold s = case viewl s of
                         x :< r -> UnfoldTuple (return x, fmap return r)

unfoldSeries :: [SeriesSeq MaybeSeq] -> [[SeriesSeq Maybe]]
unfoldSeries seriess = map (adjustSeq (fst . unUnfoldTuple)) headsAndTails : rest
    where headsAndTails = map (adjustSeq doUnfold) seriess

          rest = if any (adjustSeq' (Seq.null . snd . unUnfoldTuple)) headsAndTails
                 then []
                 else unfoldSeries (map (adjustSeq (MaybeSeq . snd . unUnfoldTuple)) headsAndTails)

          doUnfold :: MaybeSeq a -> UnfoldTuple Maybe a
          doUnfold s = case viewl (unMaybeSeq s) of
                         x :< r -> UnfoldTuple (x, r)

appendCalculatingMissing :: [AggMissingMethod Int] -> [SeriesSeq MaybeSeq] -> [SeriesSeq Maybe] -> Either String [SeriesSeq MaybeSeq]
appendCalculatingMissing methods seriess dats = Right [adjustSeq2 addDat series dat | series <- seriess | dat <- newRow]
    where addDat :: MaybeSeq a -> Maybe a -> MaybeSeq a
          addDat x y = MaybeSeq $ (unMaybeSeq x) |> y

          newRow :: [SeriesSeq Maybe]
          newRow = foldl (\accum (method, series, dat) -> accum ++ [conSeries method accum series dat]) [] (zip3 methods seriess dats)

          conSeries method accum series dat
              | adjustSeq' isNothing dat = runMissingMethod method series seriess accum
              | otherwise                = dat

-- | Infers the types of the aggregation schema declaration fields based on the target's field types
deriveAggSchema :: AggSchemaDeclaration -> [FieldDeclaration] -> Either String [FieldDeclaration]
deriveAggSchema aggSchemaD schemaFields = mapM inferField (_aggSchemaFields aggSchemaD)
    where lookupSchemaField name = lookupSchemaField' name schemaFields
              where lookupSchemaField' name [] = Left ("Field '" ++ (T.unpack . unFieldName $ name) ++ "' not found in target declaration")
                    lookupSchemaField' name (field:fields)
                        | name == _fieldDName field = Right (_fieldDType field)
                        | otherwise                 = lookupSchemaField' name fields

          inferField aggFieldD = FieldDeclaration (_aggFieldName aggFieldD) <$> inferType aggFieldD

          inferType aggFieldD = let targetN = _aggTargetFieldName aggFieldD
                                in case _aggFieldMethod aggFieldD of
                                     First -> lookupSchemaField targetN
                                     Min   -> lookupSchemaField targetN
                                     Max   -> lookupSchemaField targetN
                                     Last  -> lookupSchemaField targetN
                                     Sum   -> lookupSchemaField targetN
                                     Count -> return Integer