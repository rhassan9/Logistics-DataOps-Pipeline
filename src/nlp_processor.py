import spacy
import pandas as pd
from .logger import setup_logger

logger = setup_logger(__name__)

class NLPProcessor:
    """Class responsible for parsing unstructured text using basic NLP and regex rules."""
    
    def __init__(self):
        try:
            self.nlp = spacy.load("en_core_web_sm")
            logger.info("Successfully loaded SpaCy en_core_web_sm model.")
        except IOError:
            logger.warning("SpaCy model 'en_core_web_sm' not found. Ensure you ran `python -m spacy download en_core_web_sm`.")
            self.nlp = None

        self.category_map = {
            "Mechanical Failure": ["engine", "brake", "radiator", "transmission", "leak", "tire", "axle", "overheated", "flat", "blowout"],
            "Weather Delay": ["snow", "ice", "rain", "storm", "blizzard", "tornado", "hurricane", "flood", "weather"],
            "Traffic/Routing": ["traffic", "congestion", "accident", "construction", "detour", "stuck behind", "blocked"],
            "Driver Error": ["speeding", "logbook", "violation", "hours of service", "hos", "fatigue", "missed turn", "late start"],
            "Dock Congestion": ["loading dock", "warehouse", "waiting to load", "waiting to unload", "facility delayed", "forklift"]
        }

    def categorize_text(self, text: str) -> str:
        if pd.isna(text) or not isinstance(text, str):
            return "Uncategorized"
            
        text_lower = text.lower()
        for category, keywords in self.category_map.items():
            if any(keyword in text_lower for keyword in keywords):
                return category
                
        if self.nlp:
            try:
                doc = self.nlp(text)
            except Exception as e:
                logger.error(f"NLP processing failed on text '{text}': {e}")
                
        return "Uncategorized"

    def process_dataframe(self, df: pd.DataFrame, text_column: str, new_category_col: str) -> pd.DataFrame:
        logger.info(f"Applying NLP categorization to column: {text_column} into {new_category_col}")
        if text_column in df.columns:
            df[new_category_col] = df[text_column].apply(self.categorize_text)
        else:
            logger.warning(f"Text column {text_column} not found in dataframe for NLP processing.")
            df[new_category_col] = "Uncategorized"
        
        return df
