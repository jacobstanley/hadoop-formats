{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Data.Hadoop.SequenceFile.Parser
    ( header
    , recordBlock
    ) where

import           Control.Applicative ((<$>), (<*>))
import           Control.Monad (when, unless, replicateM)
import           Data.Attoparsec.ByteString (Parser)
import qualified Data.Attoparsec.ByteString as A
import           Data.Bits ((.|.), xor, shiftL)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import           Data.Int
import           Data.Monoid ((<>))
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Word
import           Foreign.ForeignPtr (withForeignPtr)
import           Foreign.Storable (Storable, peekByteOff)
import           System.IO.Unsafe (unsafeDupablePerformIO)
import           Text.Printf (printf)

import           Data.Hadoop.SequenceFile.Types
import           Data.Hadoop.Unsafe
import           Data.Hadoop.Writable

------------------------------------------------------------------------

#include "MachDeps.h"

------------------------------------------------------------------------

-- | Attoparsec 'Parser' for sequence file headers.
header :: Parser Header
header = do
    magic <- A.take 3
    when (magic /= "SEQ")
         (fail "not a sequence file")

    version <- A.anyWord8
    when (version /= 6)
         (fail $ "unknown version: " ++ show version)

    hdKeyType   <- textWritable
    hdValueType <- textWritable

    hdCompression      <- anyBool
    hdBlockCompression <- anyBool

    unless (hdCompression && hdBlockCompression)
           (fail "only block compressed files supported")

    hdCompressionType <- textWritable

    unless (hdCompressionType == "org.apache.hadoop.io.compress.SnappyCodec")
           (fail "only snappy compressed files supported")

    hdMetadata <- metadata
    hdSync     <- md5

    return Header{..}

metadata :: Parser [(Text, Text)]
metadata = do
    n <- fromIntegral <$> anyWord32le
    replicateM n $ (,) <$> textWritable <*> textWritable

md5 :: Parser MD5
md5 = MD5 <$> A.take 16

------------------------------------------------------------------------

-- | Attoparsec 'Parser' for sequence file record blocks.
recordBlock :: forall ck k cv v. (Writable ck k, Writable cv v) => Header -> Parser (RecordBlock k v)
recordBlock Header{..} = do
    when (hdKeyType /= keyType)
         (fail $ "expected keys of type <" <> T.unpack keyType <> "> "
              <> "but file contains <" <> T.unpack hdKeyType <> ">")

    when (hdValueType /= valueType)
         (fail $ "expected values of type <" <> T.unpack valueType <> "> "
              <> "but file contains <" <> T.unpack hdValueType <> ">")

    escape <- anyWord32le
    when (escape /= 0xffffffff)
         (fail $ "file corrupt, expected to find sync escape " ++
                 "<0xffffffff> but was " ++ printf "<0x%0x>" escape)

    sync <- md5
    when (hdSync /= sync)
         (fail $ "file corrupt, expected to find sync marker " ++
                 "<" ++ show hdSync ++ "> but was <" ++ show sync ++ ">")

    rbCount      <- anyVInt
    keyLengths   <- vintPrefixedBytes
    keys         <- vintPrefixedBytes
    valueLengths <- vintPrefixedBytes
    values       <- vintPrefixedBytes

    let rbKeys   = decodeSnappyBlock rbCount keyLengths   keys
        rbValues = decodeSnappyBlock rbCount valueLengths values

    return RecordBlock{..}
  where
    keyType   = javaType (undefined :: k)
    valueType = javaType (undefined :: v)

------------------------------------------------------------------------

textWritable :: Parser Text
textWritable = T.decodeUtf8 <$> vintPrefixedBytes
{-# INLINE textWritable #-}

vintPrefixedBytes :: Parser ByteString
vintPrefixedBytes = A.take =<< anyVInt
{-# INLINE vintPrefixedBytes #-}

anyBool :: Parser Bool
anyBool = (/= 0) <$> A.anyWord8
{-# INLINE anyBool #-}

anyVInt :: Parser Int
anyVInt = fromIntegral <$> anyVInt64
{-# INLINE anyVInt #-}

anyVInt64 :: Parser Int64
anyVInt64 = withFirst . fromIntegral =<< A.anyWord8
  where
    withFirst :: Int8 -> Parser Int64
    withFirst x | size == 1 = return (fromIntegral x)
                | otherwise = fixupSign . B.foldl' go 0 <$> A.take (size - 1)
      where
        go :: Int64 -> Word8 -> Int64
        go i b = (i `shiftL` 8) .|. fromIntegral b

        size | x >= -112 = 1
             | x <  -120 = fromIntegral (-119 - x)
             | otherwise = fromIntegral (-111 - x)

        fixupSign v = if isNegative then v `xor` (-1) else v

        isNegative = x < -120 || (x >= -112 && x < 0)
{-# INLINE anyVInt64 #-}

------------------------------------------------------------------------

#ifdef WORDS_BIGENDIAN

anyWord32le :: Parser Word32
anyWord32le = anyWord32swap
{-# INLINE anyWord32le #-}

anyWord32be :: Parser Word32
anyWord32be = anyWord32host
{-# INLINE anyWord32be #-}

#else

anyWord32le :: Parser Word32
anyWord32le = anyWord32host
{-# INLINE anyWord32le #-}

anyWord32be :: Parser Word32
anyWord32be = anyWord32swap
{-# INLINE anyWord32be #-}

#endif

anyWord32host :: Parser Word32
anyWord32host = peekBS <$> A.take 4
{-# INLINE anyWord32host #-}

anyWord32swap :: Parser Word32
anyWord32swap = byteSwap32 <$> anyWord32host
{-# INLINE anyWord32swap #-}

peekBS :: Storable a => ByteString -> a
peekBS (PS fp off _) = unsafeDupablePerformIO $ withForeignPtr fp $ \ptr -> peekByteOff ptr off
{-# INLINE peekBS #-}
