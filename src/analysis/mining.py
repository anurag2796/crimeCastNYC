import pandas as pd
from itertools import combinations
from collections import defaultdict
import logging
from src.etl.loader import get_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AssociationRuleMiner:
    def __init__(self, min_support=0.01, min_confidence=0.5, max_length=4):
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.max_length = max_length
        self.frequent_itemsets = {}
        self.rules = []

    def load_transactions(self, query=None):
        """Loads transactions from DB. Default: Last 1 year of data."""
        if query is None:
            query = """
                SELECT incident_date, incident_time, precinct_id, complaint_type 
                FROM calls_for_service 
                WHERE incident_year = (SELECT MAX(incident_year) FROM calls_for_service)
                LIMIT 50000
            """
        
        conn = get_connection()
        try:
            df = pd.read_sql(query, conn)
            # Create transactions: Group by (date, hour, precinct) -> list of complaint_types
            # This defines a "basket" as crimes happening in same precinct at same hour
            df['hour'] = pd.to_datetime(df['incident_time'].astype(str), format='%H:%M:%S').dt.hour
            transactions = df.groupby(['incident_date', 'hour', 'precinct_id'])['complaint_type'].apply(list).tolist()
            return transactions
        finally:
            conn.close()

    def get_frequent_itemsets(self, transactions):
        """Generates frequent itemsets using Apriori strategy."""
        n_transactions = len(transactions)
        logger.info(f"Mining {n_transactions} transactions...")
        
        # 1-itemsets
        counts = defaultdict(int)
        for t in transactions:
            for item in set(t):
                counts[frozenset([item])] += 1
                
        current_itemsets = {
            itemset: count 
            for itemset, count in counts.items() 
            if count / n_transactions >= self.min_support
        }
        
        self.frequent_itemsets.update(current_itemsets)
        
        k = 2
        while current_itemsets and k <= self.max_length:
            logger.info(f"Generating candidates for k={k}...")
            
            # Join step
            items = list(current_itemsets.keys())
            candidates = set()
            for i in range(len(items)):
                for j in range(i + 1, len(items)):
                    # Join if first k-2 items are same
                    l1 = sorted(list(items[i]))
                    l2 = sorted(list(items[j]))
                    if l1[:-1] == l2[:-1]:
                        candidates.add(items[i] | items[j])
            
            # Prune step & Count
            counts = defaultdict(int)
            for t in transactions:
                t_set = set(t)
                for cand in candidates:
                    if cand.issubset(t_set):
                        counts[cand] += 1
            
            current_itemsets = {
                itemset: count
                for itemset, count in counts.items()
                if count / n_transactions >= self.min_support
            }
            
            self.frequent_itemsets.update(current_itemsets)
            k += 1
            
        logger.info(f"Found {len(self.frequent_itemsets)} frequent itemsets.")
        return self.frequent_itemsets

    def generate_rules(self):
        """Generates rules from frequent itemsets."""
        logger.info("Generating association rules...")
        
        for itemset, support_count in self.frequent_itemsets.items():
            if len(itemset) < 2:
                continue
                
            support = support_count # Absolute count, or relative? Logic above mixed. 
            # support above is absolute count. Let's fix that visualization or use relative later.
            
            for i in range(1, len(itemset)):
                for antecedent in combinations(itemset, i):
                    antecedent = frozenset(antecedent)
                    consequent = itemset - antecedent
                    
                    if antecedent in self.frequent_itemsets:
                        confidence = support_count / self.frequent_itemsets[antecedent]
                        if confidence >= self.min_confidence:
                            self.rules.append({
                                'antecedent': list(antecedent),
                                'consequent': list(consequent),
                                'confidence': confidence,
                                'support_count': support_count
                            })
                            
        # Sort by confidence
        self.rules.sort(key=lambda x: x['confidence'], reverse=True)
        return self.rules

if __name__ == "__main__":
    miner = AssociationRuleMiner()
    # Mock usage
    # transactions = miner.load_transactions()
    # miner.get_frequent_itemsets(transactions)
    # rules = miner.generate_rules()
    # print(rules[:5])
    print("Mining Module Ready.")
