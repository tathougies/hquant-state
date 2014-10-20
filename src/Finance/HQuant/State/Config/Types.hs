{-# LANGUAGE GeneralizedNewtypeDeriving, TemplateHaskell, NamedFieldPuns, DeriveDataTypeable #-}
-- | Data declarations for HQuant-state configuration
module Finance.HQuant.State.Config.Types where

import Finance.HQuant.State.Types

import Control.Lens
import Control.Lens.TH

import qualified Data.Text as T
import Data.String
import Data.SafeCopy
import Data.Typeable
import Data.Acid

type Digits = Int
type Decimals = Int
data FieldType = Fixed Digits Decimals
               | Integer
               | String
                 deriving (Show, Read, Eq, Ord)

-- | Top-level declarations in configuration file.
data SeriesDeclaration = TimeSeriesDecl
                         { _tsdNamePattern       :: TSNamePattern
                         , _tsdStorageService    :: Maybe StorageServiceDeclaration
                         , _tsdSchema            :: SchemaDeclaration
                         , _tsdKeepFor           :: Duration}
                       | AggregationDecl
                         { _aggName              :: AggregationName
                         , _aggStorageService    :: Maybe StorageServiceDeclaration
                         , _aggSchema            :: AggSchemaDeclaration
                         , _aggTarget            :: TSNamePattern
                         , _aggFrequency         :: Frequency
                         , _aggPeriod            :: Duration }
                         deriving (Show, Typeable)

data StorageServiceDeclaration = S3Declaration
                                 { _s3dBucket    :: T.Text
                                 , _s3dObjPfx    :: T.Text
                                 , _s3dFrequency :: Frequency
                                 , _s3dKeepFor   :: Duration}
                               | DiskDeclaration
                                 { _ddPath       :: FilePath
                                 , _ddFrequency  :: Frequency
                                 , _ddKeepFor    :: Duration }
                                 deriving (Show)

data SchemaDeclaration = SchemaDeclaration
                         { _schemaDFields        :: [FieldDeclaration ] }
                         deriving (Show)

data AggSchemaDeclaration = AggSchemaDeclaration
                           { _aggSchemaFields    :: [AggFieldDeclaration] }
                           deriving (Show)

data AggFieldDeclaration  = AggFieldDeclaration
                            { _aggFieldName          :: FieldName
                            , _aggTargetFieldName    :: FieldName
                            , _aggFieldMethod        :: AggMethod
                            , _aggFieldMissingMethod :: AggMissingMethod FieldName }
                            deriving (Show)

data FieldDeclaration = FieldDeclaration
                        { _fieldDName            :: FieldName
                        , _fieldDType            :: FieldType }
                      deriving (Show)

deriveSafeCopy 0 'base ''SeriesDeclaration
deriveSafeCopy 0 'base ''StorageServiceDeclaration
deriveSafeCopy 0 'base ''SchemaDeclaration
deriveSafeCopy 0 'base ''FieldDeclaration
deriveSafeCopy 0 'base ''FieldType
deriveSafeCopy 0 'base ''AggFieldDeclaration
deriveSafeCopy 0 'base ''AggSchemaDeclaration

-- ** Declaration Lenses

makeLenses ''SeriesDeclaration
makeLenses ''StorageServiceDeclaration
makeLenses ''SchemaDeclaration
makeLenses ''FieldDeclaration
makeLenses ''AggFieldDeclaration
makeLenses ''AggSchemaDeclaration

sdStorageService :: Lens SeriesDeclaration SeriesDeclaration (Maybe StorageServiceDeclaration) (Maybe StorageServiceDeclaration)
sdStorageService = lens getStorageService setStorageService
    where getStorageService (TimeSeriesDecl {_tsdStorageService})  = _tsdStorageService
          getStorageService (AggregationDecl {_aggStorageService}) = _aggStorageService

          setStorageService t@(TimeSeriesDecl {}) d  = t { _tsdStorageService = d }
          setStorageService t@(AggregationDecl {}) d = t { _aggStorageService = d }

ssdFrequency :: Lens StorageServiceDeclaration StorageServiceDeclaration Frequency Frequency
ssdFrequency = lens getFrequency setFrequency
    where getFrequency (S3Declaration {_s3dFrequency}) = _s3dFrequency
          getFrequency (DiskDeclaration {_ddFrequency}) = _ddFrequency

          setFrequency t@(S3Declaration {}) d = t { _s3dFrequency = d }
          setFrequency t@(DiskDeclaration {}) d = t { _ddFrequency = d }

ssdKeepFor :: Lens StorageServiceDeclaration StorageServiceDeclaration Duration Duration
ssdKeepFor = lens getKeepFor setKeepFor
    where getKeepFor (S3Declaration {_s3dKeepFor}) = _s3dKeepFor
          getKeepFor (DiskDeclaration {_ddKeepFor}) = _ddKeepFor

          setKeepFor t@(S3Declaration {}) d = t { _s3dKeepFor = d }
          setKeepFor t@(DiskDeclaration {}) d = t { _ddKeepFor = d }
