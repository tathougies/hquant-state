module Main where

import Finance.HQuant.State

import Control.Concurrent

import Data.ByteString (ByteString)
import Data.Sequence as Seq
import Data.Set as Set hiding (foldl)
import Data.String
import Data.Acid
import Data.Acid.Remote

import Network

import System.Console.GetOpt
import System.Environment
import System.Exit
import System.Log.Logger

data HQuantStateCmdLnArgs = CmdLn
    { cmdLnConfigFiles   :: Seq FilePath
    , cmdLnStoreFile     :: FilePath
    , cmdLnSharedSecrets :: Set ByteString
    , cmdLnPortID        :: PortID }
type CmdLineM = HQuantStateCmdLnArgs -> IO HQuantStateCmdLnArgs

options :: [OptDescr CmdLineM]
options = [ Option "c" ["config"]
                   (ReqArg (\s args -> return $ args { cmdLnConfigFiles = (cmdLnConfigFiles args) |> s }) "FILE")
                   "Load the configuration specified in FILE"
          , Option "s" ["shared-secret"]
                   (ReqArg (\s args -> return $ args { cmdLnSharedSecrets = Set.insert (fromString s) (cmdLnSharedSecrets args) }) "SECRET")
                   "Set SECRET as the AcidState shared secret"
          , Option "p" ["port"]
                   (ReqArg (\s args -> return $ args { cmdLnPortID = PortNumber (fromIntegral (read s :: Int)) }) "PORT")
                   "Serve on TCP/IP port PORT"
          , Option []  ["unix-socket"]
                   (ReqArg (\s args -> return $ args { cmdLnPortID = UnixSocket s }) "SOCKET")
                   "Serve on UNIX socket SOCKET"
          , Option "v" ["verbose"]
                   (NoArg (\s -> do
                             updateGlobalLogger "Finance.HQuant.State" $ setLevel DEBUG
                             return s))
                   "Print out helpful logging messages"
          , Option "h" ["help"]
                   (NoArg (\_ -> printHelpAndExit))
                   "Display help message" ]

emptyCmdLn :: HQuantStateCmdLnArgs
emptyCmdLn = CmdLn { cmdLnConfigFiles   = Seq.empty
                   , cmdLnStoreFile     = ""
                   , cmdLnSharedSecrets = Set.empty
                   , cmdLnPortID        = PortNumber 9001 }

printHelpAndExit :: IO a
printHelpAndExit = do
  progName <- getProgName
  let usageStr = usageInfo progName options
  putStrLn usageStr
  exitWith (ExitFailure 1)

parseArgs :: IO HQuantStateCmdLnArgs
parseArgs = do
  args <- getArgs
  let (actions, nonOptions, errors) = getOpt Permute options args
      (baseCmdLn, errors') =
          case nonOptions of
            []        -> (emptyCmdLn, "No storage directory given on command line" : errors)
            [storage] -> let cmdLn' = emptyCmdLn { cmdLnStoreFile = storage }
                         in (cmdLn', errors)
            _         -> (emptyCmdLn, "Multiple storage directories on comand line" : errors)
  case errors' of
    [] -> foldl (>>=) (return baseCmdLn) actions
    _  -> do
      mapM_ putStrLn errors'
      printHelpAndExit

main :: IO ()
main = do
  cmdLnArgs <- parseArgs
  acidSt <- loadHQuantState ((cmdLnConfigFiles cmdLnArgs) `Seq.index` 0) (cmdLnStoreFile cmdLnArgs)

  let checkSecret = if Set.null (cmdLnSharedSecrets cmdLnArgs)
                    then skipAuthenticationCheck
                    else sharedSecretCheck (cmdLnSharedSecrets cmdLnArgs)

  seriess <- query acidSt TimeSeriesDeclarations
  forkIO (maintainSeries acidSt seriess)
  acidServer checkSecret (cmdLnPortID cmdLnArgs) acidSt