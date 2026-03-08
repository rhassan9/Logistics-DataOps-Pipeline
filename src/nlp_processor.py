import spacy
import pandas as pd
from .logger import setup_logger

logger = setup_logger(__name__)

class NLPProcessor:
    """Class responsible for parsing unstructured text using context-aware NLP and regex rules."""
    
    def __init__(self):
        try:
            # We must use spacy to parse the linguistic structure of the sentences
            self.nlp = spacy.load("en_core_web_sm")
            logger.info("Successfully loaded SpaCy en_core_web_sm model.")
        except IOError:
            logger.warning("SpaCy model 'en_core_web_sm' not found. Ensure you ran `python -m spacy download en_core_web_sm`.")
            self.nlp = None

    def _extract_safety_category(self, doc) -> str:
        """
        Dynamically extracts the root cause of a safety incident using Dependency Parsing.
        Example: "Severe incident involving weather" -> Extracts "weather" (pobj of 'involving')
        """
        for token in doc:
            # Look for the object of the preposition "involving" or "due to"
            if token.dep_ == "pobj" and token.head.lemma_ in ["involve", "to", "with"]:
                # Lemmatize the root cause (e.g., 'drivers' -> 'driver')
                return f"{token.lemma_.capitalize()}-Related"
                
        # Fallback: Find the most significant Noun in the sentence
        nouns = [token.lemma_.capitalize() for token in doc if token.pos_ == "NOUN" and token.lemma_ != "incident"]
        if nouns:
            return f"{nouns[-1]}-Related"
            
        return "Uncategorized Incident"

    def _extract_maintenance_category(self, doc) -> str:
        """
        Dynamically extracts the maintenance focus using Part-of-Speech tagging.
        Example: "Emergency Brake Replacement" -> Extracts "Brake"
        """
        # Look for the main NOUN that isn't a generic action word
        generic_actions = {"inspection", "repair", "replacement", "maintenance", "check"}
        
        nouns = [token.lemma_.capitalize() for token in doc if token.pos_ in ["NOUN", "PROPN"]]
        
        # Try to find a specific part or system
        specific_nouns = [n for n in nouns if n.lower() not in generic_actions]
        if specific_nouns:
            return specific_nouns[-1]
            
        # Fallback to the generic action if no specific part is found (e.g., "Routine Preventive")
        if nouns:
            return nouns[-1]
            
        # Final fallback: Look for adjectives acting as nouns (e.g., "Preventive")
        adjectives = [token.lemma_.capitalize() for token in doc if token.pos_ == "ADJ" and token.lemma_ != "routine" and token.lemma_ != "emergency" and token.lemma_ != "scheduled"]
        if adjectives:
            return adjectives[-1]
            
        return "General Maintenance"

    def categorize_text(self, text: str, context: str) -> str:
        if pd.isna(text) or not isinstance(text, str) or not self.nlp:
            return "Uncategorized"
            
        doc = self.nlp(text)
        
        if context == "safety":
            return self._extract_safety_category(doc)
        elif context == "maintenance":
            return self._extract_maintenance_category(doc)
            
        return "Uncategorized"

    def process_dataframe(self, df: pd.DataFrame, text_column: str, new_category_col: str, context: str = "maintenance") -> pd.DataFrame:
        logger.info(f"Applying {context.upper()} NLP categorization to column: {text_column} into {new_category_col}")
        
        if text_column in df.columns:
            # Apply lambda to inject the context parameter
            df[new_category_col] = df[text_column].apply(lambda x: self.categorize_text(x, context))
        else:
            logger.warning(f"Text column {text_column} not found in dataframe for NLP processing.")
            df[new_category_col] = "Uncategorized"
        
        return df
