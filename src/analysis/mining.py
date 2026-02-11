import pandas as pd
import numpy as np
import logging
from src.etl.loader import get_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AssociationRuleMiner:
    def __init__(self, min_support=0.001, min_confidence=0.01):
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.rules_df = pd.DataFrame()

    def load_transactions_df(self):
        """Loads data and prepares a one-hot encoded DataFrame for mining."""
        query = """
            SELECT incident_date, incident_time, precinct_id, complaint_type 
            FROM calls_for_service 
        """
        conn = get_connection()
        try:
            logger.info("Loading data for mining...")
            df = pd.read_sql(query, conn)
            
            # Create transaction ID
            try:
                df['hour'] = pd.to_datetime(df['incident_time'].astype(str), format='%H:%M:%S').dt.hour
            except:
                df['hour'] = df['incident_time'].astype(str).str[:2].astype(int)
                
            # Group keys
            df['transaction_id'] = (
                df['incident_date'].astype(str) + "_" + 
                df['hour'].astype(str) + "_" + 
                df['precinct_id'].astype(str)
            )
            
            logger.info("Pivoting to item records...")
            # We want a set of items per transaction. 
            # Vectorized approach: Crosstab (might be heavy if many items, but crime types are <100 usually)
            # Filter top N complaint types if too many
            top_complaints = df['complaint_type'].value_counts().head(50).index
            df_filtered = df[df['complaint_type'].isin(top_complaints)]
            
            # Crosstab: Rows=Transactions, Cols=Complaint Types
            # This counts occurrences (1 or 0)
            basket = pd.crosstab(df_filtered['transaction_id'], df_filtered['complaint_type']).clip(upper=1)
            
            logger.info(f"Matrix shape: {basket.shape}")
            return basket
        except Exception as e:
            logger.error(f"Error loading transactions: {e}")
            return None
        finally:
            conn.close()

    def mine_rules(self, basket):
        """Mines rules using matrix multiplication for co-occurrence."""
        logger.info("Calculating co-occurrence matrix...")
        
        # Support for single items (diagonal of co-occurrence if we treated it right, but simpler:)
        n_transactions = len(basket)
        item_support = basket.sum() / n_transactions
        
        # Filter items by min_support
        frequent_items = item_support[item_support >= self.min_support].index
        basket = basket[frequent_items]
        item_support = item_support[frequent_items]
        
        logger.info(f"Mining on {len(frequent_items)} frequent items...")
        
        # Co-occurrence matrix: A.T dot A
        # Since A is 0/1, A.T dot A gives count of co-occurrences
        cooccurrence = basket.T.dot(basket)
        
        rules = []
        items = cooccurrence.columns
        
        # Iterate through pair-wise co-occurrences
        # Only look at upper triangle to avoid duplicates, but we need both directions for confidence (A->B vs B->A)
        # Actually simplest is to iterate all pairs (A, B) where A != B
        
        import tqdm
        # Convert to numpy for speed
        co_matrix = cooccurrence.values
        support_vec = item_support.values
        item_names = items.tolist()
        
        count_matrix = co_matrix # Count of joint occurrences
        
        # Vectorized rule generation?
        # Confidence A->B = Count(A&B) / Count(A)
        # Lift A->B = Confidence / Support(B) = (Count(A&B)/Count(A)) / (Count(B)/N)
        #           = (Count(A&B) * N) / (Count(A) * Count(B))
        
        logger.info("Generating rules matrix...")
        for i in range(len(item_names)):
            for j in range(len(item_names)):
                if i == j: continue
                
                count_ab = count_matrix[i, j]
                if count_ab == 0: continue
                
                count_a = support_vec[i] * n_transactions
                count_b = support_vec[j] * n_transactions
                
                support_ab = count_ab / n_transactions
                
                if support_ab < self.min_support:
                    continue
                    
                confidence = count_ab / count_a
                if confidence < self.min_confidence:
                    continue
                    
                lift = (count_ab * n_transactions) / (count_a * count_b)
                
                rules.append({
                    'antecedent': item_names[i],
                    'consequent': item_names[j],
                    'support': support_ab,
                    'confidence': confidence,
                    'lift': lift
                })
                
        df_rules = pd.DataFrame(rules)
        if not df_rules.empty:
            df_rules = df_rules.sort_values('lift', ascending=False)
            
        return df_rules

if __name__ == "__main__":
    pass
