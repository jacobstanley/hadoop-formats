module Hadoop.SequenceFile.Types
    ( Header(..)
    , MD5(..)
    , RecordBlock(..)
    , rbCount
    ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.Text (Text)
import qualified Data.Vector as V
import           Text.Printf (printf)

------------------------------------------------------------------------

-- | The header of a sequence file. Contains the names of the Java classes
-- used to encode the file and potentially some metadata.
data Header = Header
    { hdKeyType         :: !Text -- ^ Package qualified class name of the key type.
    , hdValueType       :: !Text -- ^ Package qualified class name of the value type.
    , hdCompressionType :: !Text -- ^ Package qualified class name of the compression codec.
    , hdMetadata        :: ![(Text, Text)] -- ^ File metadata.
    , hdSync            :: !MD5 -- ^ The synchronization pattern used to check for
                                -- corruption throughout the file.
    } deriving (Eq, Ord, Show)

-- | An MD5 hash. Stored between each record block in a sequence file to check
-- for corruption.
newtype MD5 = MD5 { unMD5 :: ByteString }
    deriving (Eq, Ord)

-- | A block of key\/value pairs. The key at index /i/ always relates to the
-- value at index /i/. Both vectors will always be the same size.
data RecordBlock k v = RecordBlock
    { rbKeys   :: V.Vector k -- ^ The keys.
    , rbValues :: V.Vector v -- ^ The values.
    } deriving (Eq, Ord, Show)

-- | Count the number of records in this block.
rbCount :: RecordBlock k v -> Int
rbCount (RecordBlock ks _) = V.length ks

------------------------------------------------------------------------

instance Show MD5 where
    show (MD5 bs) = printf "MD5 %0x%0x%0x%0x%0x%0x"
                           (bs `B.index` 0)
                           (bs `B.index` 1)
                           (bs `B.index` 2)
                           (bs `B.index` 3)
                           (bs `B.index` 4)
                           (bs `B.index` 5)
