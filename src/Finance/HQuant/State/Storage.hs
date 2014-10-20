{-# LANGUAGE OverloadedStrings #-}
module Finance.HQuant.State.Storage
    (formatSeries, store) where

import Finance.HQuant.State.ACID hiding (aggregate)
import Finance.HQuant.State.Config
import Finance.HQuant.State.Types
import Finance.HQuant.State.Aggregations

import qualified Aws as Aws
import qualified Aws.S3 as S3

import qualified Control.Exception as E
import Control.Monad.Identity
import Control.Comonad
import Control.Monad.Trans.Resource
import Control.Concurrent.MVar
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
import Data.String

import Network.HTTP.Conduit (newManager, responseBody, Manager(..), managerConnCount, managerResponseTimeout, conduitManagerSettings)
import Network.HTTP.Client  (RequestBody(..))

import System.Log.Logger
import System.FilePath
import System.Locale
import System.Directory
import System.IO.Unsafe

moduleName = "Finance.HQuant.State.Storage"

s3AwsConfig :: Aws.Configuration
s3AwsConfig = unsafePerformIO Aws.baseConfiguration

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

getHttpMgr :: MVar (Maybe Manager) -> IO Manager
getHttpMgr st = E.uninterruptibleMask_ $ do
  mgr <- takeMVar st
  case mgr of
    Nothing -> do
             mgr <- newManager (conduitManagerSettings { managerConnCount = 3, managerResponseTimeout = Just 60000000 })
             putMVar st (Just mgr)
             return mgr
    Just mgr' -> do
             putMVar st mgr
             return mgr'

repeatOnError :: Int -> IO a -> IO a
repeatOnError left action = action `E.catch` (\e -> do
                                                putStrLn ("repeatOnError("++show left++"): " ++ show (e :: E.SomeException))
                                                if left == 1
                                                then E.throwIO e
                                                else repeatOnError (left - 1) action)

store :: MVar (Maybe Manager) -> SeriesDeclaration -> UTCTime -> SeriesName -> Maybe LBS.ByteString -> IO ()
store st d at sn Nothing    = return ()
store st d at sn (Just dat) = do
  let Just ssd = d ^. sdStorageService
  infoM moduleName $ concat [ "Store time series '"
                            , T.unpack . unSeriesName $ sn ]
  let folderPath = case d of
                     TimeSeriesDecl  {} -> T.unpack $
                                           T.drop (T.length . unTSNamePattern . _tsdNamePattern $ d) $
                                           (unSeriesName sn)
                     AggregationDecl {} -> T.unpack $
                                           T.drop (T.length . unTSNamePattern . _aggTarget $ d) $
                                           unSeriesName (extractSeriesNameFromAggName sn)
      filePath   = case d of
                     TimeSeriesDecl  {} -> let p   = T.replace "/" "_" $
                                                    (unTSNamePattern . _tsdNamePattern $ d)
                                               p'  = if "_" `T.isPrefixOf` p then T.drop 1 p else p
                                               p'' = if "_" `T.isSuffixOf` p' then T.take (T.length p' - 1) p' else p'
                                           in T.unpack p''
                     AggregationDecl {} -> T.unpack . unSeriesName . _aggName $ d

      fileName = (concat [filePath, "_", formatTime defaultTimeLocale "%Y%m%d_%0k%0M%0S" at]) <.> "csv"
  case ssd of
    DiskDeclaration {} -> do
       let folderName = _ddPath ssd </> folderPath
       infoM moduleName $ "Storing onto disk at " ++ folderName ++ ": " ++ fileName
       createDirectoryIfMissing True folderName
       LBS.writeFile (folderName </> fileName) dat
    S3Declaration {} -> do
        let bucket = _s3dBucket ssd
            fullPath = (T.unpack . _s3dObjPfx $ ssd) </> folderPath </> fileName
            putObjectReq = S3.putObject bucket (fromString fullPath) (RequestBodyLBS dat)
            putObjectReq' = putObjectReq
                            { S3.poContentType = Just "text/csv" }
        httpMgr <- getHttpMgr st
        por <- repeatOnError 3 $ runResourceT (Aws.pureAws s3AwsConfig Aws.defServiceConfig httpMgr putObjectReq')
        infoM moduleName $ "Stored onto S3 at '" ++ fullPath ++ "'. Version " ++ maybe "Nothing" T.unpack (S3.porVersionId por)
