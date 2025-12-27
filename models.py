"""Pydantic models for event validation and data structures."""

from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
from uuid import uuid4


class EventType(str, Enum):
    """Types of significant startup events."""
    USER_GROWTH = "user_growth"
    REVENUE_MILESTONE = "revenue_milestone"
    FUNDING = "funding"
    PARTNERSHIP = "partnership"
    PRODUCT_LAUNCH = "product_launch"
    HIRING = "hiring"
    ENTERPRISE_DEAL = "enterprise_deal"
    TECHNICAL_ACHIEVEMENT = "technical_achievement"
    MARKET_EXPANSION = "market_expansion"
    COMPLIANCE = "compliance"
    MEDIA_COVERAGE = "media_coverage"
    INVESTOR_INTEREST = "investor_interest"
    CUSTOMER_SUCCESS = "customer_success"
    OTHER = "other"


class EventSeverity(str, Enum):
    """Event importance level."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class SignificantEvent(BaseModel):
    """
    Model representing a significant startup event extracted by the LLM.
    
    A significant event is one in which the startup achieved something notable,
    such as user base increase, secured investment, product launch, etc.
    """
    id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType = Field(
        ..., 
        description="The category/type of the significant event"
    )
    title: str = Field(
        ..., 
        min_length=5,
        max_length=200,
        description="Brief title summarizing the event"
    )
    description: str = Field(
        ..., 
        min_length=10,
        description="Detailed description of what happened"
    )
    impact: str = Field(
        ..., 
        description="Impact or significance of this event for the startup"
    )
    metrics: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Quantitative metrics related to the event (numbers, percentages, etc.)"
    )
    timeline: str = Field(
        ..., 
        description="When this event occurred or timeline context"
    )
    source_messages: List[str] = Field(
        default_factory=list,
        description="IDs or snippets of source messages that informed this event"
    )
    severity: EventSeverity = Field(
        default=EventSeverity.MEDIUM,
        description="Importance level of this event"
    )
    stakeholders: List[str] = Field(
        default_factory=list,
        description="Key people or entities involved in this event"
    )
    created_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = Field(
        default=True,
        description="Whether this event is still active/relevant"
    )
    
    @field_validator('title')
    def title_must_be_meaningful(cls, v):
        """Ensure title is not just whitespace or generic."""
        if not v.strip():
            raise ValueError('Title cannot be empty or just whitespace')
        return v.strip()
    
    @field_validator('description')
    def description_must_be_meaningful(cls, v):
        """Ensure description is meaningful."""
        if not v.strip():
            raise ValueError('Description cannot be empty or just whitespace')
        return v.strip()
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EventExtractionResult(BaseModel):
    """Result from LLM event extraction."""
    events: List[SignificantEvent] = Field(
        default_factory=list,
        description="List of significant events extracted from messages"
    )
    total_messages_analyzed: int = Field(
        default=0,
        description="Total number of messages that were analyzed"
    )
    extraction_timestamp: datetime = Field(default_factory=datetime.now)
    summary: Optional[str] = Field(
        default=None,
        description="Brief summary of the extraction results"
    )
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class InvestorEmailRequest(BaseModel):
    """Request model for generating investor email."""
    startup_name: str = Field(
        default="ScaleFlow",
        description="Name of the startup"
    )
    events: List[SignificantEvent] = Field(
        ...,
        description="List of significant events to consolidate"
    )
    sender_name: str = Field(
        default="Alex",
        description="Name of the person sending the email"
    )
    sender_title: str = Field(
        default="CEO",
        description="Title of the sender"
    )


class InvestorEmail(BaseModel):
    """Model for generated investor outreach email."""
    subject: str = Field(
        ...,
        description="Email subject line"
    )
    body: str = Field(
        ...,
        description="Full email body with proper formatting"
    )
    highlights: List[str] = Field(
        default_factory=list,
        description="Key highlights mentioned in the email"
    )
    metrics_summary: Dict[str, Any] = Field(
        default_factory=dict,
        description="Summary of metrics mentioned"
    )
    generated_at: datetime = Field(default_factory=datetime.now)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class MessageBatch(BaseModel):
    """Batch of messages for LLM processing."""
    messages: List[Dict[str, Any]] = Field(
        ...,
        description="List of message dictionaries"
    )
    company_id: str = Field(
        ...,
        description="Company identifier for these messages"
    )
    batch_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for this batch"
    )
    chat_count: int = Field(
        default=0,
        description="Number of chat messages in the batch"
    )
    email_count: int = Field(
        default=0,
        description="Number of email messages in the batch"
    )
    created_at: datetime = Field(default_factory=datetime.now)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ProcessingStatus(BaseModel):
    """Status of the data processing pipeline."""
    total_rows_processed: int = Field(default=0)
    total_messages_ingested: int = Field(default=0)
    total_events_extracted: int = Field(default=0)
    llm_triggers: int = Field(default=0)
    current_cache_size: int = Field(default=0)
    errors: List[str] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=datetime.now)
    completed_at: Optional[datetime] = Field(default=None)
    status: str = Field(default="running")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
