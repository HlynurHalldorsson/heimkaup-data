#!/usr/bin/env python3
"""
Heimkaup.is Inventory Tracker
Tracks product inventory via the Jiffy Grocery API and monitors stock levels.
"""

import requests
import json
import os
import time
import csv
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import logging

# Import alert handlers (optional)
try:
    from heimkaup_alerts import AlertManager
    ALERTS_AVAILABLE = True
except ImportError:
    ALERTS_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('inventory_tracker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    "api_base": "https://api2.jiffygrocery.co.uk",
    "company_id": "8b4b64e1-0e88-4889-b75a-1367bfb9baea",
    "warehouse_code": "H495",
    "data_dir": "inventory_data",
    "history_file": "inventory_history.json",
    "current_file": "current_inventory.json",
    "changes_file": "inventory_changes.json",
    "check_interval_seconds": 3600,  # 1 hour
    "open_hour": 11,   # Heimkaup opens at 11:00
    "close_hour": 22,  # Heimkaup closes at 22:00
}


@dataclass
class Product:
    """Represents a product with inventory information."""
    id: int
    code: str
    name: str
    slug: str
    category_id: int
    category_name: str
    category_path: str  # e.g. "Beer > Lager > All Lagers"
    image_url: Optional[str]
    price: float
    discount_price: Optional[float]
    stock: Dict[str, int]  # warehouse_id -> quantity
    total_stock: int
    variants: List[Dict]
    last_updated: str


class HeimkaupInventoryTracker:
    """Tracks inventory for all products on Heimkaup.is"""

    def __init__(self, config: Dict = None):
        self.config = config or CONFIG
        self.data_dir = self.config["data_dir"]
        self.alert_manager = None
        self._ensure_data_dir()

    def setup_alerts(self, discord_webhook: str = None, slack_webhook: str = None,
                     email_config: Dict = None):
        """Configure alert handlers."""
        if not ALERTS_AVAILABLE:
            logger.warning("Alert module not available - alerts disabled")
            return

        self.alert_manager = AlertManager()

        if discord_webhook:
            self.alert_manager.add_discord_webhook(discord_webhook)
            logger.info("Discord alerts enabled")

        if slack_webhook:
            self.alert_manager.add_slack_webhook(slack_webhook)
            logger.info("Slack alerts enabled")

        if email_config:
            self.alert_manager.add_email(**email_config)
            logger.info("Email alerts enabled")

    def _ensure_data_dir(self):
        """Create data directory if it doesn't exist."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            logger.info(f"Created data directory: {self.data_dir}")

    def _get_file_path(self, filename: str) -> str:
        """Get full path for a data file."""
        return os.path.join(self.data_dir, filename)

    def _api_headers(self) -> Dict[str, str]:
        """Return headers for Jiffy API requests."""
        return {"x-company-id": self.config["company_id"]}

    def _api_get(self, path: str, params: Dict = None) -> Optional[Dict]:
        """Make a GET request to the Jiffy API."""
        url = f"{self.config['api_base']}/{path}"
        try:
            response = requests.get(url, headers=self._api_headers(), params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API request failed ({path}): {e}")
            return None

    def fetch_category_tree(self) -> Optional[List[Dict]]:
        """Fetch the full shop category tree from the API."""
        data = self._api_get("catalog/v1/client/categories/tree", {
            "warehouseCode": self.config["warehouse_code"],
            "landingPage[]": "shop",
            "depth": 3,
        })
        if not data:
            return None
        categories = data.get("data", [])
        logger.info(f"Fetched category tree: {len(categories)} top-level categories")
        return categories

    def _get_leaf_categories(self, items: List[Dict], parent_path: str = "") -> List[Dict]:
        """Recursively extract leaf categories with their full path."""
        leaves = []
        for item in items:
            cat = item["category"]
            path = f"{parent_path} > {cat['name']}" if parent_path else cat["name"]
            children = item.get("children", [])
            if not children:
                cat["_path"] = path
                leaves.append(cat)
            else:
                leaves.extend(self._get_leaf_categories(children, path))
        return leaves

    def fetch_category_products(self, category_id: int, category_name: str, category_path: str) -> List[Product]:
        """Fetch all products for a specific category, handling pagination."""
        products = []
        page = 1
        timestamp = datetime.now().isoformat()

        while True:
            data = self._api_get(f"catalog/v1/client/categories/{category_id}/products", {
                "warehouseCode": self.config["warehouse_code"],
                "page[size]": 100,
                "page[current]": page,
            })
            if not data:
                break

            page_products = data.get("data", {}).get("products", [])
            pagination = data.get("pagination", {})

            for p in page_products:
                stock = p.get("sellableWarehouses", {})
                total_stock = sum(stock.values()) if stock else 0
                images = p.get("images", [])
                image_url = images[0].get("url") if images else None
                products.append(Product(
                    id=p.get("id", 0),
                    code=p.get("code", ""),
                    name=p.get("name", "Unknown"),
                    slug=p.get("slug", ""),
                    category_id=p.get("categoryId", 0),
                    category_name=category_name,
                    category_path=category_path,
                    image_url=image_url,
                    price=p.get("price", 0),
                    discount_price=p.get("discountPrice"),
                    stock=stock,
                    total_stock=total_stock,
                    variants=p.get("variants", []),
                    last_updated=timestamp,
                ))

            total = pagination.get("total", 0)
            if page * 100 >= total:
                break
            page += 1

        return products

    def fetch_all_products(self) -> List[Product]:
        """Fetch all products from all leaf categories via the API."""
        tree = self.fetch_category_tree()
        if not tree:
            return []

        leaves = self._get_leaf_categories(tree)
        logger.info(f"Found {len(leaves)} leaf categories to fetch")

        seen_ids = set()
        products = []

        for leaf in leaves:
            cat_products = self.fetch_category_products(leaf["id"], leaf["name"], leaf["_path"])
            for p in cat_products:
                if p.id not in seen_ids:
                    seen_ids.add(p.id)
                    products.append(p)
            if cat_products:
                logger.info(f"  {leaf['name']}: {len(cat_products)} products")

        logger.info(f"Fetched {len(products)} unique products total")
        return products

    def load_previous_inventory(self) -> Dict[int, Dict]:
        """Load the previous inventory snapshot."""
        filepath = self._get_file_path(self.config["current_file"])
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return {p["id"]: p for p in data.get("products", [])}
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load previous inventory: {e}")
        return {}

    def save_current_inventory(self, products: List[Product]):
        """Save the current inventory snapshot."""
        filepath = self._get_file_path(self.config["current_file"])
        data = {
            "timestamp": datetime.now().isoformat(),
            "total_products": len(products),
            "products": [asdict(p) for p in products]
        }
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved current inventory to {filepath}")

    def detect_changes(self, current_products: List[Product], previous_inventory: Dict[int, Dict]) -> List[Dict]:
        """Detect inventory changes between current and previous state."""
        changes = []
        timestamp = datetime.now().isoformat()

        current_by_id = {p.id: p for p in current_products}

        # Check for changes and new products
        for product in current_products:
            prev = previous_inventory.get(product.id)

            if prev is None:
                # New product
                changes.append({
                    "type": "new_product",
                    "timestamp": timestamp,
                    "product_id": product.id,
                    "product_name": product.name,
                    "stock": product.total_stock,
                    "details": asdict(product)
                })
            else:
                # Check stock changes
                prev_stock = prev.get("total_stock", 0)
                if product.total_stock != prev_stock:
                    change_amount = product.total_stock - prev_stock
                    changes.append({
                        "type": "stock_change",
                        "timestamp": timestamp,
                        "product_id": product.id,
                        "product_name": product.name,
                        "previous_stock": prev_stock,
                        "current_stock": product.total_stock,
                        "change": change_amount,
                        "direction": "increase" if change_amount > 0 else "decrease"
                    })

                # Check price changes
                prev_price = prev.get("price", 0)
                if product.price != prev_price:
                    changes.append({
                        "type": "price_change",
                        "timestamp": timestamp,
                        "product_id": product.id,
                        "product_name": product.name,
                        "previous_price": prev_price,
                        "current_price": product.price
                    })

        # Check for removed products
        for product_id, prev_product in previous_inventory.items():
            if product_id not in current_by_id:
                changes.append({
                    "type": "product_removed",
                    "timestamp": timestamp,
                    "product_id": product_id,
                    "product_name": prev_product.get("name", "Unknown"),
                    "last_stock": prev_product.get("total_stock", 0)
                })

        return changes

    def save_changes(self, changes: List[Dict]):
        """Append changes to the changes log file."""
        if not changes:
            return

        filepath = self._get_file_path(self.config["changes_file"])

        # Load existing changes
        existing_changes = []
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    existing_changes = json.load(f)
            except (json.JSONDecodeError, IOError):
                pass

        # Append new changes
        existing_changes.extend(changes)

        # Save all changes
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(existing_changes, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved {len(changes)} changes to {filepath}")

    def save_csv(self, products: List[Product]):
        """Save inventory snapshot as CSV with timestamp in filename."""
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
        filename = f"inventory_{timestamp}.csv"
        filepath = self._get_file_path(filename)

        # Also maintain a master CSV with all historical data
        master_filepath = self._get_file_path("inventory_all.csv")
        master_exists = os.path.exists(master_filepath)

        # CSV columns
        fieldnames = [
            "timestamp", "id", "code", "name", "category_name",
            "price", "discount_price", "total_stock", "warehouse_stock"
        ]

        snapshot_time = datetime.now().isoformat()

        # Write timestamped snapshot
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for p in products:
                writer.writerow({
                    "timestamp": snapshot_time,
                    "id": p.id,
                    "code": p.code,
                    "name": p.name,
                    "category_name": p.category_name,
                    "price": p.price / 100,  # Convert from cents to ISK
                    "discount_price": p.discount_price / 100 if p.discount_price else "",
                    "total_stock": p.total_stock,
                    "warehouse_stock": json.dumps(p.stock)
                })

        logger.info(f"Saved CSV snapshot to {filepath}")

        # Append to master CSV
        with open(master_filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not master_exists:
                writer.writeheader()
            for p in products:
                writer.writerow({
                    "timestamp": snapshot_time,
                    "id": p.id,
                    "code": p.code,
                    "name": p.name,
                    "category_name": p.category_name,
                    "price": p.price / 100,
                    "discount_price": p.discount_price / 100 if p.discount_price else "",
                    "total_stock": p.total_stock,
                    "warehouse_stock": json.dumps(p.stock)
                })

        logger.info(f"Appended to master CSV: {master_filepath}")

        return filepath

    def save_history_snapshot(self, products: List[Product]):
        """Save a timestamped snapshot to history."""
        filepath = self._get_file_path(self.config["history_file"])

        # Load existing history
        history = []
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            except (json.JSONDecodeError, IOError):
                pass

        # Create snapshot summary
        snapshot = {
            "timestamp": datetime.now().isoformat(),
            "total_products": len(products),
            "total_stock": sum(p.total_stock for p in products),
            "by_category": {},
            "out_of_stock": [],
            "low_stock": []  # products with stock <= 5
        }

        # Categorize products
        for product in products:
            cat = product.category_name
            if cat not in snapshot["by_category"]:
                snapshot["by_category"][cat] = {"count": 0, "total_stock": 0}
            snapshot["by_category"][cat]["count"] += 1
            snapshot["by_category"][cat]["total_stock"] += product.total_stock

            if product.total_stock == 0:
                snapshot["out_of_stock"].append({
                    "id": product.id,
                    "name": product.name
                })
            elif product.total_stock <= 5:
                snapshot["low_stock"].append({
                    "id": product.id,
                    "name": product.name,
                    "stock": product.total_stock
                })

        history.append(snapshot)

        # Keep last 1000 snapshots
        if len(history) > 1000:
            history = history[-1000:]

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved history snapshot to {filepath}")

    def print_summary(self, products: List[Product], changes: List[Dict]):
        """Print a summary of the current inventory state."""
        print("\n" + "="*60)
        print(f"HEIMKAUP INVENTORY SUMMARY - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)

        total_stock = sum(p.total_stock for p in products)
        out_of_stock = [p for p in products if p.total_stock == 0]
        low_stock = [p for p in products if 0 < p.total_stock <= 5]

        print(f"\nTotal Products: {len(products)}")
        print(f"Total Stock Units: {total_stock}")
        print(f"Out of Stock: {len(out_of_stock)}")
        print(f"Low Stock (<=5): {len(low_stock)}")

        # Group by category
        categories = {}
        for p in products:
            if p.category_name not in categories:
                categories[p.category_name] = []
            categories[p.category_name].append(p)

        print(f"\nCategories ({len(categories)}):")
        for cat_name, cat_products in sorted(categories.items()):
            cat_stock = sum(p.total_stock for p in cat_products)
            print(f"  - {cat_name}: {len(cat_products)} products, {cat_stock} units")

        if changes:
            print(f"\n--- CHANGES DETECTED ({len(changes)}) ---")
            for change in changes[:20]:  # Show first 20 changes
                if change["type"] == "stock_change":
                    direction = "+" if change["change"] > 0 else ""
                    print(f"  [{change['type']}] {change['product_name']}: "
                          f"{change['previous_stock']} -> {change['current_stock']} "
                          f"({direction}{change['change']})")
                elif change["type"] == "price_change":
                    print(f"  [{change['type']}] {change['product_name']}: "
                          f"{change['previous_price']} -> {change['current_price']} ISK")
                elif change["type"] == "new_product":
                    print(f"  [{change['type']}] {change['product_name']} (stock: {change['stock']})")
                elif change["type"] == "product_removed":
                    print(f"  [{change['type']}] {change['product_name']}")

            if len(changes) > 20:
                print(f"  ... and {len(changes) - 20} more changes")

        if low_stock:
            print(f"\n--- LOW STOCK ALERT ---")
            for p in sorted(low_stock, key=lambda x: x.total_stock)[:10]:
                print(f"  - {p.name}: {p.total_stock} left")

        print("\n" + "="*60)

    def save_precomputed_data(self, products: List[Product], previous_inventory: Dict[int, Dict]):
        """Update pre-computed JSON files for fast API access."""
        timestamp = datetime.now().isoformat()

        # Update per-product history files
        for product in products:
            filepath = self._get_file_path(f"product_history_{product.id}.json")

            history = []
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        existing = json.load(f)
                        history = existing.get("history", [])
                except (json.JSONDecodeError, IOError):
                    pass

            history.append({
                "timestamp": timestamp,
                "stock": product.total_stock,
                "price": round(product.price / 100, 2),
            })

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump({"id": product.id, "name": product.name, "history": history}, f, ensure_ascii=False)

        # Update sales events
        if previous_inventory:
            sales_filepath = self._get_file_path("sales_events.json")

            sales_events = []
            if os.path.exists(sales_filepath):
                try:
                    with open(sales_filepath, 'r', encoding='utf-8') as f:
                        sales_events = json.load(f)
                except (json.JSONDecodeError, IOError):
                    pass

            for product in products:
                prev = previous_inventory.get(product.id)
                if prev is None:
                    continue
                stock_diff = prev.get("total_stock", 0) - product.total_stock
                if stock_diff > 0:
                    price_isk = round(product.price / 100, 2)
                    sales_events.append({
                        "timestamp": timestamp,
                        "productId": product.id,
                        "productName": product.name,
                        "unitsSold": stock_diff,
                        "pricePerUnit": price_isk,
                        "revenue": round(stock_diff * price_isk, 2),
                    })

            with open(sales_filepath, 'w', encoding='utf-8') as f:
                json.dump(sales_events, f, ensure_ascii=False)

        logger.info(f"Updated pre-computed data for {len(products)} products")

    def run_once(self) -> bool:
        """Run a single inventory check."""
        logger.info("Starting inventory check...")

        # Fetch all products via API
        products = self.fetch_all_products()
        if not products:
            logger.warning("No products found")
            return False

        # Load previous inventory and detect changes
        previous_inventory = self.load_previous_inventory()
        changes = []
        if previous_inventory:
            changes = self.detect_changes(products, previous_inventory)

        # Save everything
        self.save_current_inventory(products)
        self.save_changes(changes)
        self.save_history_snapshot(products)
        self.save_precomputed_data(products, previous_inventory)
        csv_file = self.save_csv(products)

        # Print summary
        self.print_summary(products, changes)

        # Send alerts if configured
        if self.alert_manager and changes:
            summary = {
                "total_products": len(products),
                "total_stock": sum(p.total_stock for p in products),
                "out_of_stock": len([p for p in products if p.total_stock == 0])
            }
            sent = self.alert_manager.send_alerts(changes, summary)
            logger.info(f"Sent alerts to {sent} handlers")

        logger.info("Inventory check completed successfully")

        # Push data to GitHub for cloud access
        self.push_to_github()

        return True

    def push_to_github(self):
        """Push inventory data to GitHub repository."""
        data_dir = self.config["data_dir"]
        try:
            # Change to data directory
            original_dir = os.getcwd()
            os.chdir(data_dir)

            # Check if it's a git repo
            if not os.path.exists('.git'):
                logger.warning("Data directory is not a git repository, skipping push")
                os.chdir(original_dir)
                return

            # Add, commit, and push
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            subprocess.run(['git', 'add', '-A'], check=True, capture_output=True)

            # Check if there are changes to commit
            result = subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True)
            if not result.stdout.strip():
                logger.info("No changes to push to GitHub")
                os.chdir(original_dir)
                return

            subprocess.run(
                ['git', 'commit', '-m', f'Inventory update {timestamp}'],
                check=True, capture_output=True
            )
            subprocess.run(['git', 'push'], check=True, capture_output=True)
            logger.info("Successfully pushed inventory data to GitHub")

            os.chdir(original_dir)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to push to GitHub: {e}")
            os.chdir(original_dir)
        except Exception as e:
            logger.error(f"Error pushing to GitHub: {e}")

    def _is_open(self) -> bool:
        """Check if Heimkaup is currently within operating hours."""
        now = datetime.now()
        return self.config["open_hour"] <= now.hour < self.config["close_hour"]

    def _seconds_until_open(self) -> int:
        """Calculate seconds until next opening time."""
        now = datetime.now()
        if now.hour >= self.config["close_hour"]:
            # After closing - sleep until tomorrow's opening
            tomorrow_open = now.replace(
                hour=self.config["open_hour"], minute=0, second=0, microsecond=0
            ) + __import__('datetime').timedelta(days=1)
            return int((tomorrow_open - now).total_seconds())
        else:
            # Before opening today
            today_open = now.replace(
                hour=self.config["open_hour"], minute=0, second=0, microsecond=0
            )
            return int((today_open - now).total_seconds())

    def run_continuous(self, interval: int = None):
        """Run inventory checks continuously during operating hours."""
        interval = interval or self.config["check_interval_seconds"]
        logger.info(f"Starting continuous monitoring (interval: {interval}s, "
                     f"hours: {self.config['open_hour']}:00-{self.config['close_hour']}:00)")

        while True:
            if self._is_open():
                try:
                    self.run_once()
                except Exception as e:
                    logger.error(f"Error during inventory check: {e}")

                logger.info(f"Next check in {interval // 60} minutes...")
                time.sleep(interval)
            else:
                wait = self._seconds_until_open()
                hours = wait // 3600
                mins = (wait % 3600) // 60
                logger.info(f"Heimkaup is closed. Sleeping {hours}h {mins}m until opening...")
                time.sleep(wait)


def load_config_file(config_path: str) -> Dict:
    """Load configuration from a JSON file."""
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load config file: {e}")
    return {}


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Heimkaup.is Inventory Tracker")
    parser.add_argument("--continuous", "-c", action="store_true",
                        help="Run continuously instead of once")
    parser.add_argument("--interval", "-i", type=int, default=3600,
                        help="Check interval in seconds (default: 3600)")
    parser.add_argument("--data-dir", "-d", type=str, default="inventory_data",
                        help="Directory to store data files")
    parser.add_argument("--config", type=str, default="tracker_config.json",
                        help="Path to config file (default: tracker_config.json)")
    parser.add_argument("--discord-webhook", type=str,
                        help="Discord webhook URL for alerts")
    parser.add_argument("--slack-webhook", type=str,
                        help="Slack webhook URL for alerts")

    args = parser.parse_args()

    # Load file config and merge with CLI args
    file_config = load_config_file(args.config)

    config = CONFIG.copy()
    config["data_dir"] = args.data_dir
    config["check_interval_seconds"] = args.interval

    tracker = HeimkaupInventoryTracker(config)

    # Setup alerts from CLI args or config file
    discord_webhook = args.discord_webhook or file_config.get("discord_webhook")
    slack_webhook = args.slack_webhook or file_config.get("slack_webhook")
    email_config = file_config.get("email")

    if discord_webhook or slack_webhook or email_config:
        tracker.setup_alerts(
            discord_webhook=discord_webhook,
            slack_webhook=slack_webhook,
            email_config=email_config
        )

    if args.continuous:
        tracker.run_continuous()
    else:
        tracker.run_once()


if __name__ == "__main__":
    main()
