{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | This module allows for lazy decoding of hadoop sequence files from a lazy
-- 'L.ByteString'. In the future an incremental API using strict 'S.ByteString'
-- will be provided, but for now if you need that level of control you need to
-- use the attoparsec parsers in 'Hadoop.SequenceFile.Parser' directly.
--
-- __Basic Examples__
--
-- > import           Control.Applicative ((<$>))
-- > import           Data.ByteString (ByteString)
-- > import qualified Data.ByteString.Lazy as L
-- > import qualified Data.Foldable as F
-- > import           Data.Text (Text)
-- > import qualified Data.Text.IO as T
-- > import           System.FilePath (FilePath)
-- >
-- > import           Hadoop.SequenceFile
-- >
-- > -- | Print all the keys in a sequence file.
-- > printKeys :: FilePath -> IO ()
-- > printKeys path = do
-- >     bs <- L.readFile path
-- >     let records = decode bs :: Stream (RecordBlock Text ByteString)
-- >     F.for_ records $ \rb -> do
-- >         F.mapM_ T.putStrLn (rbKeys rb)
-- >
-- > -- | Count the number of records in a sequence file.
-- > recordCount :: FilePath -> IO ()
-- > recordCount path = do
-- >     bs <- L.readFile path
-- >     let records = decode bs :: Stream (RecordBlock Text ByteString)
-- >     print $ F.sum $ rbCount <$> records
--
-- __Integration with Conduit__
--
-- > sourceRecords :: MonadIO m => FilePath -> Source m (RecordBlock Text ByteString)
-- > sourceRecords path = do
-- >     bs <- liftIO (L.readFile path)
-- >     F.traverse_ yield (decode bs)

module Hadoop.SequenceFile
    ( Stream(..)
    , Writable(..)

    , RawRecordBlock
    , RecordBlock(..)
    , rbCount

    , decode
    , rawDecode
    ) where

import           Control.Applicative ((<$>))
import qualified Data.Attoparsec.ByteString.Lazy as A
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as L
import           Data.Foldable (Foldable(..))
import           Data.Int (Int8)
import           Data.Monoid ((<>), mempty)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import           Data.Word (Word8)

import           Hadoop.SequenceFile.Parser
import           Hadoop.SequenceFile.Types

------------------------------------------------------------------------

-- | A lazy stream of values.
data Stream a = Error !String
              | Value !a (Stream a)
              | Done
    deriving (Eq, Ord, Show)

instance Functor Stream where
    fmap f (Value x s) = Value (f x) (fmap f s)
    fmap _ (Error err) = Error err
    fmap _ Done        = Done

instance Foldable Stream where
    foldMap f (Value x s) = f x <> foldMap f s
    foldMap _ _           = mempty

------------------------------------------------------------------------

-- | Equivalent to /org.apache.hadoop.io.Writable/.  Any /key/ or /value/
-- type in the Hadoop Map-Reduce framework implements this interface.
class Writable a where
    -- | Gets the package qualified name of 'a' in Java land. Does not
    -- inspect the value of 'a', simply uses it for type information.
    javaType :: a -> Text

    -- | Convert a strict 'B.ByteString' to a value.
    fromBytes :: S.ByteString -> a

    -- | Convert a value to a strict 'B.ByteString'.
    toBytes :: a -> S.ByteString

------------------------------------------------------------------------

instance Writable () where
    javaType _  = "org.apache.hadoop.io.NullWritable"
    fromBytes _ = ()
    toBytes _   = S.empty

instance Writable Text where
    javaType _               = "org.apache.hadoop.io.TextWritable"
    fromBytes bs | S.null bs = T.empty
                 | otherwise = T.decodeUtf8 $ S.drop (vintSize (S.head bs)) bs
    toBytes _                = error "Writable.Text.toBytes: not implemented"

instance Writable S.ByteString where
    javaType _ = "org.apache.hadoop.io.BytesWritable"
    fromBytes  = S.drop 4
    toBytes _  = error "Writable.Text.toBytes: not implemented"

------------------------------------------------------------------------

type RawRecordBlock = RecordBlock S.ByteString S.ByteString

vintSize :: Word8 -> Int
vintSize = go . fromIntegral
  where
    go :: Int8 -> Int
    go x | x >= -112 = 1
         | x < -120  = fromIntegral (-119 - x)
         | otherwise = fromIntegral (-111 - x)

------------------------------------------------------------------------

-- | Decode a lazy 'L.ByteString' in to a stream of record blocks.
decode :: (Writable k, Writable v) => L.ByteString -> Stream (RecordBlock k v)
decode bs = go <$> rawDecode bs
  where
    go RecordBlock{..} = RecordBlock (V.map fromBytes rbKeys)
                                     (V.map fromBytes rbValues)

-- | Decode a lazy 'L.ByteString' in to a stream of raw record blocks.
rawDecode :: L.ByteString -> Stream RawRecordBlock
rawDecode bs = case A.parse header bs of
    A.Fail _ ctx err -> mkError ctx err
    A.Done bs' hdr   -> untilEnd (recordBlock hdr) bs'

untilEnd :: A.Parser a -> L.ByteString -> Stream a
untilEnd p bs = case A.parse p bs of
    A.Fail _ ctx err -> mkError ctx err
    A.Done bs' x     -> Value x (untilEnd p bs')

mkError :: [String] -> String -> Stream a
mkError ctx err = Error (err <> "\ncontext:\n" <> concatMap indentLn ctx)
  where
    indentLn str    = "    " <> str <> "\n"
