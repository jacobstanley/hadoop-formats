{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

-- | This module allows for lazy decoding of hadoop sequence files from a lazy
-- 'L.ByteString'. In the future an incremental API using strict 'S.ByteString'
-- will be provided, but for now if you need that level of control you need to
-- use the attoparsec parsers in 'Hadoop.SequenceFile.Parser' directly.
--
-- __Basic Examples__
--
-- > import           Control.Applicative ((<$>))
-- > import qualified Data.ByteString.Lazy as L
-- > import qualified Data.Foldable as F
-- > import           Data.Int (Int32)
-- > import           Data.Text (Text)
-- > import qualified Data.Text.IO as T
-- >
-- > import           Data.Hadoop.SequenceFile
-- >
-- > -- | Print all the keys in a sequence file.
-- > printKeys :: FilePath -> IO ()
-- > printKeys path = do
-- >     bs <- L.readFile path
-- >     let records = decode bs :: Stream (RecordBlock Text Int32)
-- >     F.for_ records $ \rb -> do
-- >         F.mapM_ T.putStrLn (rbKeys rb)
-- >
-- > -- | Count the number of records in a sequence file.
-- > recordCount :: FilePath -> IO ()
-- > recordCount path = do
-- >     bs <- L.readFile path
-- >     let records = decode bs :: Stream (RecordBlock Text Int32)
-- >     print $ F.sum $ rbCount <$> records
--
-- __Integration with Conduit__
--
-- > sourceRecords :: MonadIO m => FilePath -> Source m (RecordBlock Text ByteString)
-- > sourceRecords path = do
-- >     bs <- liftIO (L.readFile path)
-- >     F.traverse_ yield (decode bs)

module Data.Hadoop.SequenceFile
    ( Stream(..)
    , Writable(..)
    , RecordBlock(..)
    , decode
    ) where

import qualified Data.Attoparsec.ByteString.Lazy as A
import qualified Data.ByteString.Lazy as L
import           Data.Foldable (Foldable(..))
import           Data.Monoid ((<>), mempty)

import           Data.Hadoop.SequenceFile.Parser
import           Data.Hadoop.SequenceFile.Types
import           Data.Hadoop.Writable

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

-- | Decode a lazy 'L.ByteString' in to a stream of record blocks.
decode :: (Writable ck k, Writable cv v) => L.ByteString -> Stream (RecordBlock k v)
decode bs = case A.parse header bs of
    A.Fail _ ctx err -> mkError ctx err
    A.Done bs' hdr   -> untilEnd (recordBlock hdr) bs'

untilEnd :: A.Parser a -> L.ByteString -> Stream a
untilEnd p bs = case A.parse p bs of
    A.Fail _ ctx err -> mkError ctx err
    A.Done bs' x     -> Value x (untilEnd p bs')

mkError :: [String] -> String -> Stream a
mkError [] err  = Error err
mkError ctx err = Error (err <> "\ncontext:\n" <> concatMap indentLn ctx)
  where
    indentLn str    = "    " <> str <> "\n"
