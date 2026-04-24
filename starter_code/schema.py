from pydantic import BaseModel, Field, AliasChoices, ConfigDict
from typing import Optional, Dict, Any
from datetime import datetime

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================
# Your task is to define the Unified Schema for all sources.
# This is v1. Note: A breaking change is coming at 11:00 AM!

class UnifiedDocument(BaseModel):
    # v2 contract with backward-compatible input aliases for v1 fields.
    model_config = ConfigDict(populate_by_name=True)

    doc_id: str = Field(
        validation_alias=AliasChoices("doc_id", "document_id"),
        serialization_alias="doc_id",
    )
    text: str = Field(
        validation_alias=AliasChoices("text", "content"),
        serialization_alias="text",
    )
    source: str = Field(
        validation_alias=AliasChoices("source", "source_type"),
        serialization_alias="source",
    )
    owner: Optional[str] = Field(
        default="Unknown",
        validation_alias=AliasChoices("owner", "author"),
        serialization_alias="owner",
    )
    created_at: Optional[datetime] = Field(
        default=None,
        validation_alias=AliasChoices("created_at", "timestamp"),
        serialization_alias="created_at",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("metadata", "source_metadata"),
        serialization_alias="metadata",
    )
    schema_version: str = "2.0"

    def to_v2_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json", by_alias=True)

    def to_v1_dict(self) -> Dict[str, Any]:
        return {
            "document_id": self.doc_id,
            "content": self.text,
            "source_type": self.source,
            "author": self.owner,
            "timestamp": self.created_at,
            "source_metadata": self.metadata,
        }
