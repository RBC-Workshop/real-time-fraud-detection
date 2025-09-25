"""
Test Data Generators for Fraud Detection Testing.

SCRUM-25: Provides synthetic and schema-compliant test data generation
for comprehensive testing of the fraud detection pipeline.
"""

import random
import datetime
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
import uuid


@dataclass
class TransactionGenerator:
    """Generator for realistic transaction test data."""
    
    CATEGORIES = [
        "grocery_pos", "gas_transport", "shopping_net", "entertainment", 
        "food_dining", "personal_care", "health_fitness", "travel", 
        "kids_pets", "shopping_pos", "misc_net", "misc_pos"
    ]
    
    MERCHANT_PATTERNS = {
        "grocery_pos": ["Walmart", "Target", "Kroger", "Safeway", "Whole Foods"],
        "gas_transport": ["Shell", "Exxon", "BP", "Chevron", "Mobil"],
        "shopping_net": ["Amazon", "eBay", "Etsy", "Wayfair", "Overstock"],
        "entertainment": ["Netflix", "Spotify", "AMC Theaters", "GameStop"],
        "food_dining": ["McDonald's", "Starbucks", "Subway", "Pizza Hut", "KFC"],
        "personal_care": ["CVS", "Walgreens", "Ulta", "Sephora"],
        "health_fitness": ["Planet Fitness", "LA Fitness", "GNC"],
        "travel": ["Expedia", "Booking.com", "Uber", "Lyft", "Delta"],
        "kids_pets": ["Toys R Us", "PetSmart", "Petco"],
        "shopping_pos": ["Best Buy", "Home Depot", "Macy's", "Nordstrom"],
        "misc_net": ["PayPal", "Venmo", "Square"],
        "misc_pos": ["7-Eleven", "Dollar General", "Costco"]
    }
    
    US_REGIONS = {
        "northeast": {"lat_range": (40.0, 45.0), "long_range": (-80.0, -70.0)},
        "southeast": {"lat_range": (25.0, 35.0), "long_range": (-90.0, -75.0)},
        "midwest": {"lat_range": (35.0, 45.0), "long_range": (-100.0, -80.0)},
        "southwest": {"lat_range": (25.0, 40.0), "long_range": (-125.0, -100.0)},
        "west": {"lat_range": (35.0, 50.0), "long_range": (-125.0, -110.0)}
    }
    
    @classmethod
    def generate_transaction(cls, cc_num: Optional[str] = None, fraud_probability: float = 0.1) -> Dict[str, Any]:
        """Generate a single realistic transaction."""
        if cc_num is None:
            cc_num = cls._generate_credit_card_number()
        
        category = random.choice(cls.CATEGORIES)
        merchant = random.choice(cls.MERCHANT_PATTERNS[category])
        
        amount_ranges = {
            "grocery_pos": (10.0, 200.0),
            "gas_transport": (20.0, 100.0),
            "shopping_net": (15.0, 500.0),
            "entertainment": (5.0, 50.0),
            "food_dining": (5.0, 80.0),
            "personal_care": (10.0, 150.0),
            "health_fitness": (20.0, 100.0),
            "travel": (50.0, 2000.0),
            "kids_pets": (10.0, 200.0),
            "shopping_pos": (25.0, 1000.0),
            "misc_net": (5.0, 500.0),
            "misc_pos": (5.0, 100.0)
        }
        
        min_amt, max_amt = amount_ranges.get(category, (10.0, 100.0))
        
        if random.random() < fraud_probability:
            amt = random.uniform(max_amt * 0.8, max_amt * 3.0)  # Higher amounts for fraud
        else:
            amt = random.uniform(min_amt, max_amt)
        
        region = random.choice(list(cls.US_REGIONS.keys()))
        region_data = cls.US_REGIONS[region]
        
        customer_lat = random.uniform(*region_data["lat_range"])
        customer_long = random.uniform(*region_data["long_range"])
        
        if random.random() < fraud_probability:
            fraud_region = random.choice(list(cls.US_REGIONS.keys()))
            fraud_region_data = cls.US_REGIONS[fraud_region]
            merch_lat = random.uniform(*fraud_region_data["lat_range"])
            merch_long = random.uniform(*fraud_region_data["long_range"])
        else:
            lat_offset = random.uniform(-0.5, 0.5)
            long_offset = random.uniform(-0.5, 0.5)
            merch_lat = customer_lat + lat_offset
            merch_long = customer_long + long_offset
        
        base_time = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
        trans_time = base_time.strftime("%Y-%m-%d %H:%M:%S")
        
        return {
            "cc_num": cc_num,
            "trans_time": trans_time,
            "trans_num": cls._generate_transaction_id(),
            "category": category,
            "merchant": merchant,
            "amt": round(amt, 2),
            "lat": round(customer_lat, 4),
            "long": round(customer_long, 4),
            "merch_lat": round(merch_lat, 4),
            "merch_long": round(merch_long, 4),
            "first": cls._generate_first_name(),
            "last": cls._generate_last_name()
        }
    
    @classmethod
    def generate_transaction_batch(cls, batch_size: int, unique_customers: Optional[int] = None, 
                                 fraud_rate: float = 0.1) -> List[Dict[str, Any]]:
        """Generate a batch of transactions with controlled fraud rate."""
        if unique_customers is None:
            unique_customers = max(1, batch_size // 10)  # 10 transactions per customer on average
        
        credit_cards = [cls._generate_credit_card_number() for _ in range(unique_customers)]
        
        transactions = []
        fraud_count = 0
        target_fraud_count = int(batch_size * fraud_rate)
        
        for i in range(batch_size):
            cc_num = random.choice(credit_cards)
            
            is_fraud_transaction = fraud_count < target_fraud_count and random.random() < 0.5
            fraud_prob = 0.9 if is_fraud_transaction else 0.05
            
            transaction = cls.generate_transaction(cc_num, fraud_prob)
            transactions.append(transaction)
            
            if is_fraud_transaction:
                fraud_count += 1
        
        return transactions
    
    @classmethod
    def _generate_credit_card_number(cls) -> str:
        """Generate a realistic-looking credit card number."""
        prefixes = ["4111", "4000", "5555", "5105", "3782", "3714"]
        prefix = random.choice(prefixes)
        
        remaining_digits = 16 - len(prefix)
        suffix = ''.join([str(random.randint(0, 9)) for _ in range(remaining_digits)])
        
        return prefix + suffix
    
    @classmethod
    def _generate_transaction_id(cls) -> str:
        """Generate unique transaction ID."""
        return f"T{random.randint(100000, 999999)}"
    
    @classmethod
    def _generate_first_name(cls) -> str:
        """Generate realistic first name."""
        names = [
            "John", "Jane", "Michael", "Sarah", "David", "Lisa", "Robert", "Mary",
            "James", "Patricia", "William", "Jennifer", "Richard", "Linda", "Joseph",
            "Elizabeth", "Thomas", "Barbara", "Christopher", "Susan", "Charles", "Jessica"
        ]
        return random.choice(names)
    
    @classmethod
    def _generate_last_name(cls) -> str:
        """Generate realistic last name."""
        names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson"
        ]
        return random.choice(names)


@dataclass
class CustomerGenerator:
    """Generator for customer test data."""
    
    @classmethod
    def generate_customer(cls, cc_num: Optional[str] = None) -> Dict[str, Any]:
        """Generate a single customer record."""
        if cc_num is None:
            cc_num = TransactionGenerator._generate_credit_card_number()
        
        today = datetime.date.today()
        min_age = 18
        max_age = 80
        
        birth_year = today.year - random.randint(min_age, max_age)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)  # Avoid month-end issues
        
        dob = f"{birth_year}-{birth_month:02d}-{birth_day:02d}"
        
        region = random.choice(list(TransactionGenerator.US_REGIONS.keys()))
        region_data = TransactionGenerator.US_REGIONS[region]
        
        lat = random.uniform(*region_data["lat_range"])
        long = random.uniform(*region_data["long_range"])
        
        return {
            "cc_num": cc_num,
            "dob": dob,
            "lat": round(lat, 4),
            "long": round(long, 4)
        }
    
    @classmethod
    def generate_customer_batch(cls, customer_count: int) -> List[Dict[str, Any]]:
        """Generate a batch of customer records."""
        customers = []
        used_cc_nums = set()
        
        for _ in range(customer_count):
            cc_num = TransactionGenerator._generate_credit_card_number()
            while cc_num in used_cc_nums:
                cc_num = TransactionGenerator._generate_credit_card_number()
            
            used_cc_nums.add(cc_num)
            customer = cls.generate_customer(cc_num)
            customers.append(customer)
        
        return customers
    
    @classmethod
    def generate_matching_customers(cls, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate customer records that match the credit card numbers in transactions."""
        unique_cc_nums = list(set(t["cc_num"] for t in transactions))
        return [cls.generate_customer(cc_num) for cc_num in unique_cc_nums]


class ScenarioGenerator:
    """Generator for specific testing scenarios."""
    
    @classmethod
    def generate_fraud_scenario(cls, scenario_type: str, transaction_count: int = 100) -> Tuple[List[Dict], List[Dict]]:
        """Generate specific fraud detection scenarios."""
        if scenario_type == "high_amount_fraud":
            return cls._generate_high_amount_fraud_scenario(transaction_count)
        elif scenario_type == "geographic_anomaly":
            return cls._generate_geographic_anomaly_scenario(transaction_count)
        elif scenario_type == "rapid_transactions":
            return cls._generate_rapid_transaction_scenario(transaction_count)
        elif scenario_type == "mixed_normal":
            return cls._generate_mixed_normal_scenario(transaction_count)
        else:
            raise ValueError(f"Unknown scenario type: {scenario_type}")
    
    @classmethod
    def _generate_high_amount_fraud_scenario(cls, count: int) -> Tuple[List[Dict], List[Dict]]:
        """Generate scenario with high-amount fraudulent transactions."""
        transactions = []
        customers = []
        
        customer_count = max(1, count // 20)  # Fewer customers, more transactions per customer
        customers = CustomerGenerator.generate_customer_batch(customer_count)
        cc_nums = [c["cc_num"] for c in customers]
        
        fraud_count = int(count * 0.3)  # 30% fraud rate
        
        for i in range(count):
            cc_num = random.choice(cc_nums)
            
            if i < fraud_count:
                transaction = TransactionGenerator.generate_transaction(cc_num, fraud_probability=1.0)
                transaction["amt"] = random.uniform(1000.0, 5000.0)  # Very high amounts
            else:
                transaction = TransactionGenerator.generate_transaction(cc_num, fraud_probability=0.0)
            
            transactions.append(transaction)
        
        return transactions, customers
    
    @classmethod
    def _generate_geographic_anomaly_scenario(cls, count: int) -> Tuple[List[Dict], List[Dict]]:
        """Generate scenario with geographically anomalous transactions."""
        transactions = []
        customers = []
        
        customer_count = max(1, count // 15)
        customers = CustomerGenerator.generate_customer_batch(customer_count)
        cc_nums = [c["cc_num"] for c in customers]
        
        fraud_count = int(count * 0.25)  # 25% fraud rate
        
        for i in range(count):
            cc_num = random.choice(cc_nums)
            customer = next(c for c in customers if c["cc_num"] == cc_num)
            
            if i < fraud_count:
                transaction = TransactionGenerator.generate_transaction(cc_num, fraud_probability=0.0)
                
                customer_region = cls._get_region_for_coordinates(customer["lat"], customer["long"])
                other_regions = [r for r in TransactionGenerator.US_REGIONS.keys() if r != customer_region]
                fraud_region = random.choice(other_regions)
                fraud_region_data = TransactionGenerator.US_REGIONS[fraud_region]
                
                transaction["merch_lat"] = random.uniform(*fraud_region_data["lat_range"])
                transaction["merch_long"] = random.uniform(*fraud_region_data["long_range"])
            else:
                transaction = TransactionGenerator.generate_transaction(cc_num, fraud_probability=0.0)
                lat_offset = random.uniform(-0.2, 0.2)
                long_offset = random.uniform(-0.2, 0.2)
                transaction["merch_lat"] = customer["lat"] + lat_offset
                transaction["merch_long"] = customer["long"] + long_offset
            
            transactions.append(transaction)
        
        return transactions, customers
    
    @classmethod
    def _generate_rapid_transaction_scenario(cls, count: int) -> Tuple[List[Dict], List[Dict]]:
        """Generate scenario with rapid successive transactions (potential fraud)."""
        transactions = []
        customers = []
        
        customer_count = max(1, count // 25)
        customers = CustomerGenerator.generate_customer_batch(customer_count)
        cc_nums = [c["cc_num"] for c in customers]
        
        base_time = datetime.datetime.now()
        
        for i in range(count):
            cc_num = random.choice(cc_nums)
            
            if i % 10 < 3:  # 30% rapid transactions
                time_offset = random.randint(0, 300)  # Within 5 minutes
                trans_time = base_time + datetime.timedelta(seconds=time_offset)
            else:
                time_offset = random.randint(0, 86400 * 7)  # Within a week
                trans_time = base_time + datetime.timedelta(seconds=time_offset)
            
            transaction = TransactionGenerator.generate_transaction(cc_num, fraud_probability=0.1)
            transaction["trans_time"] = trans_time.strftime("%Y-%m-%d %H:%M:%S")
            transactions.append(transaction)
        
        return transactions, customers
    
    @classmethod
    def _generate_mixed_normal_scenario(cls, count: int) -> Tuple[List[Dict], List[Dict]]:
        """Generate mixed scenario with normal transaction patterns."""
        transactions = TransactionGenerator.generate_transaction_batch(count, fraud_rate=0.05)
        customers = CustomerGenerator.generate_matching_customers(transactions)
        return transactions, customers
    
    @classmethod
    def _get_region_for_coordinates(cls, lat: float, long: float) -> str:
        """Determine which US region contains the given coordinates."""
        for region, bounds in TransactionGenerator.US_REGIONS.items():
            lat_min, lat_max = bounds["lat_range"]
            long_min, long_max = bounds["long_range"]
            
            if lat_min <= lat <= lat_max and long_min <= long <= long_max:
                return region
        
        return "northeast"  # Default fallback
