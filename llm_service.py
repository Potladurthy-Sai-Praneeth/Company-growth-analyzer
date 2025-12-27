"""LLM Service using LangChain and Google Gemini for event extraction."""

import os
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.messages import SystemMessage, HumanMessage

from models import (
    SignificantEvent, 
    EventExtractionResult, 
    InvestorEmail, 
    EventType,
    EventSeverity
)
from utils import setup_logging

load_dotenv()

logger = setup_logging(__name__, "llm_service")


EVENT_EXTRACTION_SYSTEM_PROMPT = """You are an expert information processing agent specializing in analyzing startup communications to identify significant business events. Your role is to help investors track startup growth and milestones by carefully analyzing messages and emails.

## Your Expertise:
- Deep understanding of startup metrics (MRR, ARR, NRR, churn, DAU, etc.)
- Knowledge of funding rounds, investor relations, and due diligence processes
- Technical understanding of software development, scaling, and infrastructure
- Experience with enterprise sales, partnerships, and customer success

## What Constitutes a Significant Event:
A significant event is one in which the startup achieved something notable, demonstrated growth, or reached a milestone. Examples include:

1. **User/Customer Growth**: Increases in user base, DAU, MAU, customer acquisition
2. **Revenue Milestones**: MRR/ARR achievements, revenue growth, improved unit economics
3. **Funding**: Investment interest, term sheets, funding rounds, investor meetings
4. **Enterprise Deals**: Large contract negotiations, enterprise customer wins
5. **Product Launches**: New features, product releases, successful launches (e.g., ProductHunt)
6. **Technical Achievements**: Performance improvements, scalability milestones, infrastructure wins
7. **Partnerships**: Strategic partnerships, integrations, collaborations
8. **Hiring/Team Growth**: Key hires, team expansion
9. **Compliance/Security**: SOC2, security certifications, audit completions
10. **Market Expansion**: New markets, geographic expansion, vertical expansion

## Your Task:
Analyze the provided messages (Slack chats and emails) to extract significant events. For each event:
- Accurately note the timeline and context
- Extract specific metrics and numbers when mentioned
- Identify the stakeholders involved
- Assess the impact and importance
- Categorize by event type and severity

## Output Format:
Return a JSON object with the following structure:
{{
    "events": [
        {{
            "event_type": "one of: user_growth, revenue_milestone, funding, partnership, product_launch, hiring, enterprise_deal, technical_achievement, market_expansion, compliance, media_coverage, investor_interest, customer_success, other",
            "title": "Brief title (5-200 chars)",
            "description": "Detailed description of what happened",
            "impact": "Impact or significance for the startup",
            "metrics": {{"key": "value"}} or null,
            "timeline": "When this occurred or timeline context",
            "source_messages": ["brief snippet or identifier of source messages"],
            "severity": "high, medium, or low",
            "stakeholders": ["names of people/entities involved"]
        }}
    ],
    "summary": "Comprehensive yet concise summary of findings"
}}

## Important Guidelines:
- Be thorough but avoid duplicates - consolidate related messages into single events
- Focus on concrete achievements, not plans or intentions (unless significant)
- Extract specific numbers and metrics whenever available
- Prioritize events that would interest investors
- Be accurate - only extract information that is explicitly stated
- For each event, try to provide context from multiple related messages if available"""


# System prompt for investor email generation
INVESTOR_EMAIL_SYSTEM_PROMPT = """You are an expert startup communications specialist who crafts compelling investor outreach emails. Your emails help startups secure meetings with potential investors by effectively communicating growth, traction, and opportunities.

## Your Expertise:
- Deep understanding of what investors look for in startups
- Ability to present metrics and achievements compellingly
- Knowledge of proper email etiquette and professional communication
- Understanding of Series A/B fundraising dynamics

## Email Guidelines:
1. **Subject Line**: Compelling, specific, and creates urgency/interest
2. **Opening**: Hook with the most impressive metric or achievement
3. **Body Structure**:
   - Lead with traction and growth metrics
   - Highlight key milestones and achievements
   - Mention enterprise customers or notable partnerships
   - Brief mention of team and technical capabilities
   - Clear ask for a meeting

4. **Tone**: Professional but confident, data-driven yet personable
5. **Length**: Concise - investors are busy (aim for 200-400 words)
6. **Formatting**: Use bullet points for metrics, proper paragraphs for narrative

## Metrics to Highlight (when available):
- MRR/ARR and growth rate
- User/customer growth
- Net Revenue Retention (NRR)
- Notable customer logos
- Technical metrics (performance, scale)
- Team growth and key hires

## Output Format:
Return a JSON object with the following structure:
{{
    "subject": "Email subject line",
    "body": "Full email body with proper formatting (use markdown for structure)",
    "highlights": ["List of key highlights mentioned"],
    "metrics_summary": {{"metric_name": "value"}}
}}

## Important:
- Make the email feel personalized, not templated
- Use specific numbers and achievements
- Create a compelling narrative arc
- End with a clear call-to-action (meeting request)
- The email should be ready to send with minimal edits"""


class LLMService:
    """
    LLM Service for event extraction and investor email generation using Gemini Pro.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize LLM Service.
        
        Args:
            api_key: Google API key. If not provided, uses GOOGLE_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("Google API key required. Set GOOGLE_API_KEY environment variable.")
        
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-3-pro-preview",
            google_api_key=self.api_key,
            temperature=0.0,
            convert_system_message_to_human=True
        )
        
        self.json_parser = JsonOutputParser()
        
        logger.info("LLMService initialized with Gemini 3 Pro Preview model")
    
    def _format_messages_for_analysis(self, messages: List[Dict[str, Any]]) -> str:
        """
        Format messages for LLM analysis.
        
        Args:
            messages: List of message dictionaries.
            
        Returns:
            Formatted string of messages.
        """
        formatted = []
        
        for i, msg in enumerate(messages, 1):
            # Determine message type
            metadata = msg.get('metadata', {})
            topic = metadata.get('topic', 'unknown')
            source = metadata.get('source', 'unknown')
            
            if topic == 'chat' or 'text' in msg:
                # Slack message
                sender = msg.get('user', 'Unknown')
                content = msg.get('text', '')
                timestamp = msg.get('ts', '')
                formatted.append(
                    f"[{i}] CHAT ({source}) - {sender}\n"
                    f"Time: {timestamp}\n"
                    f"Message: {content}\n"
                )
            else:
                # Email message
                sender = msg.get('sender_email', metadata.get('sender', 'Unknown'))
                subject = msg.get('subject', 'No Subject')
                content = msg.get('snippet', msg.get('content', ''))
                formatted.append(
                    f"[{i}] EMAIL ({source}) - From: {sender}\n"
                    f"Subject: {subject}\n"
                    f"Content: {content}\n"
                )
        
        return "\n---\n".join(formatted)
    
    async def extract_events(
        self,
        messages: List[Dict[str, Any]],
        company_id: str = "default"
    ) -> EventExtractionResult:
        """
        Extract significant events from messages using LLM.
        
        Args:
            messages: List of message dictionaries (Slack and Gmail).
            company_id: Company identifier.
            
        Returns:
            EventExtractionResult with extracted events.
        """
        try:
            # Format messages for analysis
            formatted_messages = self._format_messages_for_analysis(messages)
            
            # Count message types
            chat_count = sum(1 for m in messages if m.get('metadata', {}).get('topic') == 'chat' or 'text' in m)
            email_count = len(messages) - chat_count
            
            # Create prompt
            human_message = f"""Analyze the following {len(messages)} messages ({chat_count} chats, {email_count} emails) from company '{company_id}' and extract all significant events.
                                ## Messages to Analyze:
                                {formatted_messages}
                                ---
                                Please extract all significant events from these messages following the format specified in your instructions. Focus on concrete achievements, milestones, and events that would be relevant to investors.
                                """

            # Call LLM
            response = await self.llm.ainvoke([
                SystemMessage(content=EVENT_EXTRACTION_SYSTEM_PROMPT),
                HumanMessage(content=human_message)
            ])
            
            # Parse response
            response_text = response.content
            
            # Extract JSON from response
            try:
                # Handle Gemini's response format which returns a list of content blocks
                # e.g., [{'type': 'text', 'text': '```json\n{...}\n```', 'extras': {...}}]
                if isinstance(response_text, list):
                    # Extract text from the first text block
                    for block in response_text:
                        if isinstance(block, dict) and block.get('type') == 'text':
                            response_text = block.get('text', '')
                            break
                    else:
                        # If no text block found, convert list to string
                        response_text = str(response_text)
                
                if isinstance(response_text, dict):
                    result_data = response_text
                else:
                    # Try to find JSON in the response
                    if "```json" in response_text:
                        json_start = response_text.find("```json") + 7
                        json_end = response_text.find("```", json_start)
                        response_text = response_text[json_start:json_end].strip()
                    elif "```" in response_text:
                        json_start = response_text.find("```") + 3
                        json_end = response_text.find("```", json_start)
                        response_text = response_text[json_start:json_end].strip()
                    
                    result_data = json.loads(response_text)
            except json.JSONDecodeError as e:
                # If JSON parsing fails, try to extract structured data
                logger.warning(f"Failed to parse JSON response: {e}, attempting fallback parsing")
                result_data = {"events": [], "summary": "Failed to parse LLM response"}
            
            # Convert to Pydantic models
            events = []
            for event_data in result_data.get('events', []):
                try:
                    # Map event type
                    event_type_str = event_data.get('event_type', 'other').lower().replace(' ', '_')
                    try:
                        event_type = EventType(event_type_str)
                    except ValueError:
                        event_type = EventType.OTHER
                    
                    # Map severity
                    severity_str = event_data.get('severity', 'medium').lower()
                    try:
                        severity = EventSeverity(severity_str)
                    except ValueError:
                        severity = EventSeverity.MEDIUM
                    
                    event = SignificantEvent(
                        event_type=event_type,
                        title=event_data.get('title', 'Unknown Event'),
                        description=event_data.get('description', ''),
                        impact=event_data.get('impact', ''),
                        metrics=event_data.get('metrics'),
                        timeline=event_data.get('timeline', 'Unknown'),
                        source_messages=event_data.get('source_messages', []),
                        severity=severity,
                        stakeholders=event_data.get('stakeholders', [])
                    )
                    events.append(event)
                except Exception as e:
                    logger.warning(f"Failed to parse event: {e}")
                    continue
            
            result = EventExtractionResult(
                events=events,
                total_messages_analyzed=len(messages),
                summary=result_data.get('summary', f"Extracted {len(events)} events from {len(messages)} messages")
            )
            
            logger.info(f"Extracted {len(events)} events from {len(messages)} messages")
            return result
            
        except Exception as e:
            logger.error(f"Failed to extract events: {e}")
            raise
    
    async def generate_investor_email(
        self,
        events: List[SignificantEvent],
        startup_name: str = "ScaleFlow",
        sender_name: str = "Alex",
        sender_title: str = "CEO"
    ) -> InvestorEmail:
        """
        Generate an investor outreach email based on significant events.
        
        Args:
            events: List of SignificantEvent models.
            startup_name: Name of the startup.
            sender_name: Name of the email sender.
            sender_title: Title of the sender.
            
        Returns:
            InvestorEmail with generated content.
        """
        try:
            # Format events for the prompt
            events_summary = self._format_events_for_email(events)
            
            # Create prompt
            human_message = f"""Generate a compelling investor outreach email for {startup_name} based on the following significant events and achievements:

## Startup: {startup_name}
## Sender: {sender_name}, {sender_title}
## Number of Events: {len(events)}

## Significant Events and Achievements:

{events_summary}

---

Please generate a professional investor outreach email that:
1. Opens with the most impressive achievement or metric
2. Highlights key growth indicators and milestones
3. Mentions notable customers or partnerships
4. Requests a meeting to discuss the opportunity
5. Uses proper formatting with markdown for structure

The email should be compelling, data-driven, and ready to send with minimal edits."""

            # Call LLM
            response = await self.llm.ainvoke([
                SystemMessage(content=INVESTOR_EMAIL_SYSTEM_PROMPT),
                HumanMessage(content=human_message)
            ])
            
            # Parse response
            response_text = response.content
            
            # Handle Gemini's response format (list of content blocks)
            if isinstance(response_text, list):
                # Extract text from content blocks
                for block in response_text:
                    if isinstance(block, dict) and block.get('type') == 'text':
                        response_text = block.get('text', '')
                        break
                else:
                    response_text = str(response_text)
            
            # Extract JSON from response
            try:
                if "```json" in response_text:
                    json_start = response_text.find("```json") + 7
                    json_end = response_text.find("```", json_start)
                    response_text = response_text[json_start:json_end].strip()
                elif "```" in response_text:
                    json_start = response_text.find("```") + 3
                    json_end = response_text.find("```", json_start)
                    response_text = response_text[json_start:json_end].strip()
                
                result_data = json.loads(response_text)
            except json.JSONDecodeError:
                logger.warning("Failed to parse JSON response for email generation")
                # Return raw response as email body
                result_data = {
                    "subject": f"{startup_name} - Growth Update & Meeting Request",
                    "body": response.content,
                    "highlights": [],
                    "metrics_summary": {}
                }
            
            email = InvestorEmail(
                subject=result_data.get('subject', f'{startup_name} - Investor Update'),
                body=result_data.get('body', ''),
                highlights=result_data.get('highlights', []),
                metrics_summary=result_data.get('metrics_summary', {})
            )
            
            logger.info(f"Generated investor email with subject: {email.subject}")
            return email
            
        except Exception as e:
            logger.error(f"Failed to generate investor email: {e}")
            raise
    
    def _format_events_for_email(self, events: List[SignificantEvent]) -> str:
        """
        Format events for email generation prompt.
        
        Args:
            events: List of SignificantEvent models.
            
        Returns:
            Formatted string of events.
        """
        formatted = []
        
        # Group events by type and severity
        high_priority = [e for e in events if e.severity == EventSeverity.HIGH]
        medium_priority = [e for e in events if e.severity == EventSeverity.MEDIUM]
        low_priority = [e for e in events if e.severity == EventSeverity.LOW]
        
        # Format high priority events first
        for priority_name, priority_events in [
            ("HIGH PRIORITY", high_priority),
            ("MEDIUM PRIORITY", medium_priority),
            ("OTHER", low_priority)
        ]:
            if priority_events:
                formatted.append(f"\n### {priority_name} EVENTS:\n")
                for event in priority_events:
                    event_str = f"""
**{event.title}** ({event.event_type})
- Description: {event.description}
- Impact: {event.impact}
- Timeline: {event.timeline}
- Stakeholders: {', '.join(event.stakeholders) if event.stakeholders else 'N/A'}
"""
                    if event.metrics:
                        metrics_str = ", ".join(f"{k}: {v}" for k, v in event.metrics.items())
                        event_str += f"- Metrics: {metrics_str}\n"
                    
                    formatted.append(event_str)
        
        return "\n".join(formatted)
    
    async def health_check(self) -> bool:
        """
        Perform health check on LLM service.
        
        Returns:
            True if healthy, False otherwise.
        """
        try:
            response = await self.llm.ainvoke([
                HumanMessage(content="Respond with just 'OK' if you're working correctly.")
            ])
            return "OK" in response.content.upper()
        except Exception as e:
            logger.error(f"LLM health check failed: {e}")
            return False
