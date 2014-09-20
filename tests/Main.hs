import           Control.Applicative ((<$>))
import qualified Data.ByteString.Lazy as L
import qualified Data.Foldable as F
import           Data.Int (Int32)
import           Data.Text (Text)
import qualified Data.Text.IO as T

import           Hadoop.SequenceFile

main :: IO ()
main = do
    printKeys "./tests/text-int.seq"
    recordCount "./tests/text-int.seq"

-- | Print all the keys in a sequence file.
printKeys :: FilePath -> IO ()
printKeys path = do
    bs <- L.readFile path
    let records = failOnError (decode bs) :: Stream (RecordBlock Text Int32)
    F.for_ records $ \rb -> do
        F.mapM_ T.putStrLn (rbKeys rb)

-- | Count the number of records in a sequence file.
recordCount :: FilePath -> IO ()
recordCount path = do
    bs <- L.readFile path
    let records = decode bs :: Stream (RecordBlock Text Int32)
    print $ F.sum $ rbCount <$> records

failOnError :: Stream a -> Stream a
failOnError (Error err) = error err
failOnError x           = x
