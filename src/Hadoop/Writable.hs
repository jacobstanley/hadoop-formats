{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# OPTIONS_GHC -w #-}

module Hadoop.Writable
    ( Writable(..)
    , Collection(..)
    ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as S
import           Data.Int (Int8, Int16, Int32, Int64)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Word (Word8)

import qualified Data.Vector as V
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Unboxed as U

------------------------------------------------------------------------

type family Collection a
type instance Collection ()         = U.Vector ()
type instance Collection Text       = V.Vector Text
type instance Collection ByteString = V.Vector ByteString

------------------------------------------------------------------------

-- | Equivalent to a collection of /org.apache.hadoop.io.Writable/. Any
-- serializable /key/ or /value/ type in the Hadoop Map-Reduce framework
-- implements this interface.
class Collection a ~ c a => Writable c a where
    -- | Gets the package qualified name of 'a' in Java land. Does not
    -- inspect the value of 'a', simply uses it for type information.
    javaType :: a -> Text

    -- | Convert a strict 'B.ByteString' and a list of lengths to a vector.
    fromBytes :: ByteString   -- ^ the encoded bytes
              -> U.Vector Int -- ^ a list of lengths describing how long each value in the encoded bytes is
              -> c a          -- ^ the decoded collection of values

------------------------------------------------------------------------

instance Writable U.Vector () where
    javaType _     = "org.apache.hadoop.io.NullWritable"
    fromBytes _ ls = U.replicate (U.length ls) ()

instance Writable V.Vector ByteString where
    javaType _      = "org.apache.hadoop.io.BytesWritable"
    fromBytes bs ls = split (S.drop 4) bs ls

instance Writable V.Vector Text where
    javaType _      = "org.apache.hadoop.io.TextWritable"
    fromBytes bs ls = split go bs ls
      where
        go bs' | S.null bs' = T.empty
               | otherwise  = T.decodeUtf8 $ S.drop (vintSize (S.head bs')) bs'

------------------------------------------------------------------------

split :: (S.ByteString -> a) -> S.ByteString -> U.Vector Int -> V.Vector a
split f bs lens = snd $ U.foldl' (splitOne f) (bs, V.empty) lens

splitOne :: (S.ByteString -> a) -> (S.ByteString, V.Vector a) -> Int -> (ByteString, V.Vector a)
splitOne f (bs, v) l = let (vbs, bs') = S.splitAt l bs
                       in (bs', v `V.snoc` f vbs)

vintSize :: Word8 -> Int
vintSize = go . fromIntegral
  where
    go :: Int8 -> Int
    go x | x >= -112 = 1
         | x < -120  = fromIntegral (-119 - x)
         | otherwise = fromIntegral (-111 - x)
