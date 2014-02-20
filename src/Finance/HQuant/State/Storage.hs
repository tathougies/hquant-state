{-# LANGUAGE OverloadedStrings #-}
module Finance.HQuant.State.Storage
    (formatSeries, store) where

import Finance.HQuant.State.ACID hiding (aggregate)
import Finance.HQuant.State.Config
import Finance.HQuant.State.Types

import Control.Monad.Identity
import Control.Comonad
import Control.Lens hiding ((<.>))

import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as LBS
import qualified Data.Sequence as Seq
import qualified Data.Csv as CSV
import Data.Sequence (Seq, viewl, viewr, ViewL(..), ViewR(..))
import Data.Function
import Data.Ord
import Data.Maybe
import Data.List
import Data.Csv
import Data.Time
import Data.Time.Clock
import Data.Acid
import Data.ByteString (ByteString)

import System.Log.Logger
import System.FilePath
import System.Locale
import System.Directory

moduleName = "Finance.HQuant.State.Storage"

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

formatSeries :: [FieldName] -> Seq UTCTime -> [SeriesSeq Seq] -> Maybe LBS.ByteString
formatSeries fields times dats
    | Seq.null times = Nothing
    | otherwise      = let unfolded  = unfoldSeriesI dats
                           rows      = map (record . map encodeRow) ((map (StringSeq . return . TE.encodeUtf8 .unFieldName) fields):unfolded)

                           encodeRow s = case extractSeq s of
                                           FixedQD r -> BS.pack . show $ (fromRational r :: Double)
                                           IntegerQD i -> BS.pack . show $ i
                                           StringQD s -> s
                       in Just . CSV.encode $ rows

store :: SeriesDeclaration -> UTCTime -> SeriesName -> Maybe LBS.ByteString -> IO ()
store d at sn Nothing    = return ()
store d at sn (Just dat) = do
  let Just ssd = d ^. sdStorageService
  infoM moduleName $ concat [ "Store time series '"
                            , T.unpack . unSeriesName $ sn ]
  let folderPath = case d of
                     TimeSeriesDecl  {} -> T.unpack $
                                           T.drop (T.length . unTSNamePattern . _tsdNamePattern $ d) $
                                           (unSeriesName sn)
                     AggregationDecl {} -> T.unpack $
                                           T.drop (T.length . unTSNamePattern . _aggTarget $ d) $
                                           (unSeriesName sn)
      filePath   = case d of
                     TimeSeriesDecl  {} -> let p   = T.replace "/" "_" $
                                                    (unTSNamePattern . _tsdNamePattern $ d)
                                               p'  = if "_" `T.isPrefixOf` p then T.drop 1 p else p
                                               p'' = if "_" `T.isSuffixOf` p then T.take (T.length p - 1) p else p
                                           in T.unpack p''
                     AggregationDecl {} -> T.unpack . unSeriesName . _aggName $ d
  case ssd of
    DiskDeclaration {} -> do
       let folderName = _ddPath ssd </> folderPath
           fileName = (concat [filePath, "_", formatTime defaultTimeLocale "%Y%m%d_%0k%0M%0S" at]) <.> "csv"
       infoM moduleName $ "Storing onto disk at " ++ folderName ++ ": " ++ fileName
       createDirectoryIfMissing True folderName
       LBS.writeFile (folderName </> fileName) dat
    _ -> return ()

