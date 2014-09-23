{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Data.Hadoop.Writable
    ( Writable(..)
    , Collection
    , Decoder(..)

    , split
    , vintSize
    , bytesToVector
    ) where

import           Control.Monad (replicateM_)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import           Data.Int (Int8, Int16, Int32, Int64)
import           Data.STRef
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Word (Word8)
import           Foreign.ForeignPtr (castForeignPtr)
import           Foreign.Storable (sizeOf)

import qualified Data.Vector as V
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as M
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Storable as S

------------------------------------------------------------------------

-- | Equivalent to the java interface /org.apache.hadoop.io.Writable/.
-- All serializable /key/ or /value/ types in the Hadoop Map-Reduce
-- framework implement this interface.
class Collection a ~ c a => Writable c a where
    -- | Gets the package qualified name of 'a' in Java land. Does not
    -- inspect the value of 'a', simply uses it for type information.
    javaType :: a -> Text

    -- | Gets a decoder for this writable type.
    decoder :: Decoder (c a)

-- | A specialized decoder for different types of writable.
data Decoder a = Variable (ByteString -> U.Vector Int -> a) -- ^ The slowest. Variable length data.
               | LE16 (ByteString -> a) -- ^ All values are 16-bit little endian.
               | LE32 (ByteString -> a) -- ^ All values are 32-bit little endian.
               | LE64 (ByteString -> a) -- ^ All values are 64-bit little endian.
               | BE16 (ByteString -> a) -- ^ All values are 16-bit big endian.
               | BE32 (ByteString -> a) -- ^ All values are 32-bit big endian.
               | BE64 (ByteString -> a) -- ^ All values are 64-bit big endian.
    deriving (Functor)

------------------------------------------------------------------------

type family Collection a
type instance Collection ()         = U.Vector ()
type instance Collection Int16      = U.Vector Int16
type instance Collection Int32      = U.Vector Int32
type instance Collection Int64      = U.Vector Int64
type instance Collection Float      = U.Vector Float
type instance Collection Double     = U.Vector Double
type instance Collection Text       = V.Vector Text
type instance Collection ByteString = V.Vector ByteString

------------------------------------------------------------------------

instance Writable U.Vector () where
    javaType _ = "org.apache.hadoop.io.NullWritable"
    decoder    = Variable $ \_ ls -> U.replicate (U.length ls) ()

instance Writable U.Vector Int16 where
    javaType _ = "org.apache.hadoop.io.ShortWritable"
    decoder    = BE16 bytesToVector

instance Writable U.Vector Int32 where
    javaType _ = "org.apache.hadoop.io.IntWritable"
    decoder    = BE32 bytesToVector

instance Writable U.Vector Int64 where
    javaType _ = "org.apache.hadoop.io.LongWritable"
    decoder    = BE64 bytesToVector

instance Writable U.Vector Float where
    javaType _ = "org.apache.hadoop.io.FloatWritable"
    decoder    = BE32 bytesToVector

instance Writable U.Vector Double where
    javaType _ = "org.apache.hadoop.io.DoubleWritable"
    decoder    = BE64 bytesToVector

instance Writable V.Vector ByteString where
    javaType _ = "org.apache.hadoop.io.BytesWritable"
    decoder    = Variable $ \bs ls -> split (B.drop 4) bs ls

instance Writable V.Vector Text where
    javaType _ = "org.apache.hadoop.io.Text"
    decoder    = Variable $ \bs ls -> split go bs ls
      where
        go bs' | B.null bs' = T.empty
               | otherwise  = T.decodeUtf8 $ B.drop (vintSize (B.head bs')) bs'

------------------------------------------------------------------------

bytesToVector :: forall a. (S.Storable a, U.Unbox a) => ByteString -> U.Vector a
bytesToVector (PS bs off nbytes) = U.convert
                                 $ S.unsafeFromForeignPtr (castForeignPtr bs) off
                                 $ nbytes `div` sizeOf (undefined :: a)

split :: G.Vector v a => (ByteString -> a) -> ByteString -> U.Vector Int -> v a
split f bs lens = G.create $ do
    let numRecs = U.length lens
    v <- M.unsafeNew numRecs
    offRef <- newSTRef 0
    idxRef <- newSTRef 0

    replicateM_ numRecs $ do
        idx <- readSTRef idxRef
        off <- readSTRef offRef

        let len   = U.unsafeIndex lens idx
            slice = unsafeSlice off len bs

        writeSTRef idxRef (idx + 1)
        writeSTRef offRef (off + len)

        M.unsafeWrite v idx (f slice)

    return v

unsafeSlice :: Int -> Int -> ByteString -> ByteString
unsafeSlice off len (PS ptr poff _) = PS ptr (poff + off) len
{-# INLINE unsafeSlice #-}

vintSize :: Word8 -> Int
vintSize = go . fromIntegral
  where
    go :: Int8 -> Int
    go x | x >= -112 = 1
         | x < -120  = fromIntegral (-119 - x)
         | otherwise = fromIntegral (-111 - x)
{-# INLINE vintSize #-}
