"""MongoDB backend for SignLedger."""

from datetime import datetime
from typing import Optional, Iterator, Dict, Any
import logging

try:
    from pymongo import MongoClient, ASCENDING, DESCENDING, IndexModel
    from pymongo.errors import DuplicateKeyError, PyMongoError
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False

from .base import StorageBackend
from ..core.exceptions import StorageError
from ..core.ledger import Entry

logger = logging.getLogger(__name__)


class MongoDBBackend(StorageBackend):
    """MongoDB storage backend for SignLedger."""
    
    def __init__(
        self,
        connection_string: str = "mongodb://localhost:27017/",
        database_name: str = "signledger",
        collection_name: str = "entries",
        **kwargs
    ):
        super().__init__(**kwargs)
        
        if not HAS_PYMONGO:
            raise ImportError("pymongo is required for MongoDB backend. Install with: pip install signledger[mongodb]")
        
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        
        # Initialize MongoDB client
        self._client = MongoClient(connection_string, **kwargs)
        self._db = self._client[database_name]
        self._collection = self._db[collection_name]
        
        # Create indexes
        self.create_indexes()
    
    def create_indexes(self) -> None:
        """Create necessary indexes for performance."""
        indexes = [
            IndexModel([("id", ASCENDING)], unique=True),
            IndexModel([("hash", ASCENDING)], unique=True),
            IndexModel([("timestamp", DESCENDING)]),
            IndexModel([("previous_hash", ASCENDING)]),
            IndexModel([("data.event", ASCENDING)]),  # For common queries
            IndexModel([("data.user", ASCENDING)]),   # For common queries
        ]
        
        try:
            self._collection.create_indexes(indexes)
        except PyMongoError as e:
            logger.warning(f"Failed to create some indexes: {e}")
    
    def append_entry(self, entry: Entry) -> None:
        """Append entry to the ledger."""
        try:
            document = entry.to_dict()
            # MongoDB doesn't like datetime objects in _id
            document["_id"] = entry.id
            
            self._collection.insert_one(document)
            
        except DuplicateKeyError:
            raise StorageError(
                f"Entry with ID {entry.id} already exists",
                "append",
                self.name
            )
        except PyMongoError as e:
            raise StorageError(
                f"Failed to append entry: {e}",
                "append",
                self.name
            )
    
    def get_entry(self, entry_id: str) -> Optional[Entry]:
        """Get entry by ID."""
        try:
            document = self._collection.find_one({"id": entry_id})
            
            if document:
                # Remove MongoDB's _id field
                document.pop("_id", None)
                return Entry.from_dict(document)
            
            return None
            
        except PyMongoError as e:
            logger.error(f"Failed to get entry {entry_id}: {e}")
            return None
    
    def get_entries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Iterator[Entry]:
        """Get entries within time range."""
        try:
            # Build query
            query = {}
            
            if start_time or end_time:
                timestamp_query = {}
                if start_time:
                    timestamp_query["$gte"] = start_time
                if end_time:
                    timestamp_query["$lte"] = end_time
                query["timestamp"] = timestamp_query
            
            # Execute query
            cursor = self._collection.find(query).sort("timestamp", ASCENDING)
            
            if offset:
                cursor = cursor.skip(offset)
            
            if limit:
                cursor = cursor.limit(limit)
            
            # Yield entries
            for document in cursor:
                document.pop("_id", None)
                yield Entry.from_dict(document)
                
        except PyMongoError as e:
            logger.error(f"Failed to get entries: {e}")
            return
    
    def get_latest_entry(self) -> Optional[Entry]:
        """Get the most recent entry."""
        try:
            document = self._collection.find_one(
                sort=[("timestamp", DESCENDING)]
            )
            
            if document:
                document.pop("_id", None)
                return Entry.from_dict(document)
            
            return None
            
        except PyMongoError as e:
            logger.error(f"Failed to get latest entry: {e}")
            return None
    
    def get_oldest_entry(self) -> Optional[Entry]:
        """Get the oldest entry."""
        try:
            document = self._collection.find_one(
                sort=[("timestamp", ASCENDING)]
            )
            
            if document:
                document.pop("_id", None)
                return Entry.from_dict(document)
            
            return None
            
        except PyMongoError as e:
            logger.error(f"Failed to get oldest entry: {e}")
            return None
    
    def count_entries(self) -> int:
        """Get total number of entries."""
        try:
            return self._collection.count_documents({})
        except PyMongoError as e:
            logger.error(f"Failed to count entries: {e}")
            return 0
    
    def get_size(self) -> int:
        """Get approximate storage size in bytes."""
        try:
            stats = self._db.command("collStats", self.collection_name)
            return stats.get("size", 0)
        except PyMongoError as e:
            logger.error(f"Failed to get collection size: {e}")
            return 0
    
    def close(self) -> None:
        """Close MongoDB connection."""
        if self._client:
            self._client.close()
    
    def verify_storage(self) -> bool:
        """Verify storage integrity."""
        try:
            # Check if we can connect and query
            self._client.admin.command('ping')
            
            # Verify indexes exist
            indexes = list(self._collection.list_indexes())
            required_indexes = {"id_1", "hash_1", "timestamp_-1"}
            existing_indexes = {idx["name"] for idx in indexes}
            
            if not required_indexes.issubset(existing_indexes):
                logger.warning("Some required indexes are missing")
                self.create_indexes()
            
            return True
            
        except PyMongoError as e:
            logger.error(f"Storage verification failed: {e}")
            return False
    
    # MongoDB-specific methods
    
    def query_by_data(self, query: Dict[str, Any], limit: Optional[int] = None) -> Iterator[Entry]:
        """Query entries by data fields (MongoDB-specific feature)."""
        try:
            # Prefix query keys with "data."
            mongo_query = {f"data.{k}": v for k, v in query.items()}
            
            cursor = self._collection.find(mongo_query).sort("timestamp", ASCENDING)
            
            if limit:
                cursor = cursor.limit(limit)
            
            for document in cursor:
                document.pop("_id", None)
                yield Entry.from_dict(document)
                
        except PyMongoError as e:
            logger.error(f"Failed to query by data: {e}")
            return
    
    def aggregate(self, pipeline: list) -> list:
        """Run aggregation pipeline (MongoDB-specific feature)."""
        try:
            return list(self._collection.aggregate(pipeline))
        except PyMongoError as e:
            logger.error(f"Failed to run aggregation: {e}")
            return []