module Finance.HQuant.State.Config
    ( module Finance.HQuant.State.Config.Types
    , readConfigFile
    ) where

import Finance.HQuant.State.Types
import Finance.HQuant.State.Config.Types

import Control.Applicative hiding (many, (<|>))
import Control.Lens
import Control.Monad.Identity

import qualified Data.Text as T
import Data.Either
import Data.String

import qualified Text.Parsec.Token as Tok
import Text.Parsec hiding (Parser)
import Text.Parsec.Text
import Text.Parsec.Token (GenLanguageDef(..), LanguageDef(..))

type ConfParseState = ()
type ConfParser = GenParser ConfParseState

confLangDef :: GenLanguageDef T.Text ConfParseState Identity
confLangDef = LanguageDef
              { commentStart    = ""
              , commentEnd      = ""
              , commentLine     = "#"
              , nestedComments  = False
              , identStart      = letter <|> char '_'
              , identLetter     = alphaNum <|> char '_'
              , opStart         = fail "Operators not supported"
              , opLetter        = opStart confLangDef
              , reservedNames   = []
              , reservedOpNames = []
              , caseSensitive   = False }

-- Token definitions
confTokParser = Tok.makeTokenParser confLangDef
identifier    = Tok.identifier confTokParser
symbol        = Tok.symbol confTokParser
braces        = Tok.braces confTokParser
stringLiteral = Tok.stringLiteral confTokParser
natural       = Tok.natural confTokParser
integer       = Tok.integer confTokParser
whiteSpace    = Tok.whiteSpace confTokParser
lexeme        = Tok.lexeme confTokParser
parens        = Tok.parens confTokParser
comma         = Tok.comma confTokParser

timeUnit :: ConfParser TimeUnit
timeUnit = try (symbol "years"   *> pure Years)   <|>
           try (symbol "year"    *> pure Years)   <|>
           try (symbol "months"  *> pure Months)  <|>
           try (symbol "month"   *> pure Months)  <|>
           try (symbol "weeks"   *> pure Weeks)   <|>
           try (symbol "week"    *> pure Weeks)   <|>
           try (symbol "days"    *> pure Days)    <|>
           try (symbol "day"     *> pure Days)    <|>
           try (symbol "hours"   *> pure Hours)   <|>
           try (symbol "hour"    *> pure Hours)   <|>
           try (symbol "minutes" *> pure Minutes) <|>
           try (symbol "minute"  *> pure Minutes) <|>
           try (symbol "seconds" *> pure Seconds) <|>
           try (symbol "second"  *> pure Seconds)

frequency :: ConfParser Frequency
frequency = symbol "daily"   *> pure (Every 1 Days)   <|>
            symbol "hourly"  *> pure (Every 1 Hours)  <|>
            symbol "weekly"  *> pure (Every 1 Weeks)  <|>
            symbol "monthly" *> pure (Every 1 Months) <|>
            symbol "every"   *>
                   (Every <$> (fromIntegral <$> (natural <|> return 1))
                          <*> timeUnit)

duration :: ConfParser Duration
duration = (try (symbol "forever") *> pure Forever) <|>
           (For <$> (fromIntegral <$> natural)
                <*> timeUnit)

storageService :: ConfParser StorageServiceDeclaration
storageService = (mkStorageService =<< braces (many storageServiceInner))
    where storageServiceInner :: ConfParser (Either String (StorageServiceDeclaration -> StorageServiceDeclaration))
          storageServiceInner = ssType <|> ssFrequency <|> ssKeepForP <|> s3BucketP <|> s3ObjPfxP <|> dDirP

          ssType      = Left <$> (symbol "type" *> stringLiteral)
          ssFrequency = Right <$> (symbol "frequency" *> ((ssdFrequency .~) <$> frequency))
          ssKeepForP  = Right <$> (symbol "keep"      *> ((ssdKeepFor   .~) <$> duration))

          s3BucketP = Right <$> (try (symbol "s3-bucket") *>
                                 ((s3dBucket .~) . fromString <$> stringLiteral <?> "S3 bucket name"))
          s3ObjPfxP = Right <$> (try (symbol "s3-object-prefix") *>
                                 ((s3dObjPfx .~) . fromString <$> stringLiteral <?> "S3 bucket name"))
          dDirP     = Right <$> (symbol "data-dir"  *>
                                  ((ddPath .~) . fromString <$> stringLiteral <?> "Data directory name"))

          mkStorageService :: [Either String (StorageServiceDeclaration -> StorageServiceDeclaration)] -> ConfParser StorageServiceDeclaration
          mkStorageService decls = do
              defaults <- case lefts decls of
                            ["S3"]   -> return $
                                        S3Declaration
                                        { _s3dBucket = error "No S3 bucket given in config"
                                        , _s3dObjPfx = fromString ""
                                        , _s3dFrequency = error "No frequency given in config"
                                        , _s3dKeepFor = Forever }
                            ["disk"] -> return $
                                        DiskDeclaration
                                        { _ddPath = error "No data directory given in config"
                                        , _ddFrequency = error "No frequency given in config"
                                        , _ddKeepFor = Forever }
                            [x]      -> fail ("Unknown storage service '" ++ x ++"' given")
                            _        -> fail ("Storage services require exactly one type declaration")
              return (foldr ($) defaults (rights decls))

aggregation :: ConfParser SeriesDeclaration
aggregation = try (symbol "aggregation") *>
              stringLiteral >>= (\name -> mkAggregation name <$> braces (many aggregationInner))
    where aggregationInner = aggStorageServiceP <|> aggTargetP <|> aggFrequencyP <|>
                             aggPeriodP <|> aggSchemaP

          aggStorageServiceP = try (symbol "storage") *> symbol "service" *> ((sdStorageService .~) . Just <$> storageService)
          aggSchemaP    = try (symbol "schema")    *> ((aggSchema .~) <$> aggSchemaParser)
          aggPeriodP    = try (symbol "period")    *> ((aggPeriod .~) <$> lexeme duration)
          aggFrequencyP = try (symbol "frequency") *> ((aggFrequency .~) <$> frequency)
          aggTargetP    = try (symbol "target")    *> ((aggTarget .~) . fromString <$> stringLiteral)

          mkAggregation name = foldr ($) (defaultAggregation name)
          defaultAggregation name = AggregationDecl
                                    { _aggName           = fromString name
                                    , _aggStorageService = Nothing
                                    , _aggSchema         = error "No schema supplied to aggregation"
                                    , _aggTarget         = error "No target supplied to aggregation"
                                    , _aggFrequency      = error "No frequency supplied to aggreg<ation"
                                    , _aggPeriod         = error "No period supplied to aggregation" }

aggSchemaParser :: ConfParser AggSchemaDeclaration
aggSchemaParser = AggSchemaDeclaration <$> braces (many aggSchemaInner)
    where aggSchemaInner = do
            aggType <- aggTypeP
            fieldName <- FieldName . fromString <$> identifier
            inners <- try (braces (many aggSchemaOption)) <|> (pure [])
            targetFieldName <- symbol "on" *> (FieldName . fromString <$> identifier)
            let baseAggregation = AggFieldDeclaration
                                  { _aggFieldName          = fieldName
                                  , _aggTargetFieldName    = targetFieldName
                                  , _aggFieldMethod        = aggType
                                  , _aggFieldMissingMethod = AMMIgnore }
            return (foldr ($) baseAggregation inners)

          aggTypeP       = try (symbol "Last")  *> pure Last  <|>
                           try (symbol "Max")   *> pure Max   <|>
                           try (symbol "Min")   *> pure Min   <|>
                           try (symbol "First") *> pure First <|>
                           try (symbol "Sum")   *> pure Sum   <?> "Aggregation Method"

          aggSchemaOption = missingP <?> "aggregation schema option"
          missingP = symbol "missing" *> ((aggFieldMissingMethod .~) <$> missingMethodP)

          missingMethodP = try (symbol "ignore") *> pure AMMIgnore <|>
                           try (symbol "lag")    *> lagP           <|>
                           try (symbol "use")    *> useP
          lagP           = parens (AMMLag <$> (FieldName . fromString <$> identifier <* comma)
                                          <*> (fromIntegral <$> integer))
          useP           = parens (AMMUse <$> (FieldName . fromString <$> identifier))

schema :: ConfParser SchemaDeclaration
schema = SchemaDeclaration <$> braces (many schemaInner)
    where schemaInner = flip FieldDeclaration <$> fieldType
                                              <*> fieldName

          fieldName = FieldName . fromString <$> identifier
          fieldType = try (symbol "Integer") *> pure Integer <|>
                      try (symbol "String")  *> pure String <|>
                      fixedType <?> "Field type"
          fixedType = try (symbol "Fixed") *>
                      parens (Fixed <$> (fromIntegral <$> natural <* comma)
                                    <*> (fromIntegral <$> natural))

timeSeriesDecl :: ConfParser SeriesDeclaration
timeSeriesDecl = try (symbol "timeseries") *>
                 stringLiteral >>= \name -> mkTimeSeries name <$> braces (many tsInner)
    where tsInner = tsStorageServiceP <|> tsSchemaP <|> tsKeepForP

          tsStorageServiceP = try (symbol "storage") *> symbol "service" *> ((sdStorageService .~) . Just <$> storageService)
          tsSchemaP  = try (symbol "schema") *> ((tsdSchema .~) <$> schema)
          tsKeepForP = try (symbol "keep")   *> ((tsdKeepFor .~) <$> duration)

          mkTimeSeries name = foldr ($) (defaultTimeSeries name)
          defaultTimeSeries name = TimeSeriesDecl
                                   { _tsdNamePattern    = fromString name
                                   , _tsdStorageService = Nothing
                                   , _tsdSchema         = SchemaDeclaration []
                                   , _tsdKeepFor        = Forever }

topLevel = whiteSpace *>
           many1 (aggregation <|> timeSeriesDecl)

readConfigFile :: FilePath -> IO (Either ParseError [SeriesDeclaration])
readConfigFile filePath = parse topLevel filePath . fromString <$> readFile filePath