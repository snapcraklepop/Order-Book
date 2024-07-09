import arrow
import csv
import heapq
from collections import defaultdict
import sys

class OrderBook:
    def __init__(self):
        self.buy_orders = defaultdict(dict)
        self.sell_orders = defaultdict(dict)
        self.trades = []
        self.start_time = None
        self.removed_orders = set()


    def process_message(self, message):
        localtime, exchtime, msgtype, symbol, exch, price, size, side, ref, oldref, mpid, esd, platform = message

        try:
            size = int(size)
        except ValueError:
            return  # Skip processing this message

        if msgtype == 'add':
            order = {'ref': ref, 'price': float(price), 'size': size, 'exchtime': exchtime}
            if side == 'B':
                self.buy_orders[ref] = order
            elif side == 'S':
                self.sell_orders[ref] = order

        elif msgtype == 'remove':
            self.removed_orders.add(ref)
            if side == 'B' and ref in self.buy_orders:
                del self.buy_orders[ref]
            elif side == 'S' and ref in self.sell_orders:
                del self.sell_orders[ref]

        elif msgtype == 'trade':
            self.trades.append((exchtime, float(price), size))
            self.removed_orders.add(ref)
            if side == 'B' and ref in self.buy_orders:
                if size >= self.buy_orders[ref]['size']:
                    del self.buy_orders[ref]
                else:
                    self.buy_orders[ref]['size'] -= size
            elif side == 'S' and ref in self.sell_orders:
                if size >= self.sell_orders[ref]['size']:
                    del self.sell_orders[ref]
                else:
                    self.sell_orders[ref]['size'] -= size

        elif msgtype == 'replace':
            self.removed_orders.add(oldref)
            if oldref in self.buy_orders:
                self.buy_orders.pop(oldref)
                self.buy_orders[ref] = {'ref': ref, 'price': float(price), 'size': size, 'exchtime': exchtime}
            elif oldref in self.sell_orders:
                self.sell_orders.pop(oldref)
                self.sell_orders[ref] = {'ref': ref, 'price': float(price), 'size': size, 'exchtime': exchtime}


    def total_trade_volume(self):
        return sum(size for exchtime, price, size in self.trades)

    def top_price_levels(self, side, n=5, thirty_minutes_ago=None):
        price_groups = defaultdict(int)
        problematic_exchtime = None
        top_levels = []

        try:
            if side == 'sell':
                for ref, order in self.sell_orders.items():
                    try:
                        order_time = arrow.get(order['exchtime'], 'YYYY-MM-DD HH:mm:ss.SSSSSSSSS')
                        if ref not in self.removed_orders and (
                                thirty_minutes_ago is None or order_time >= thirty_minutes_ago):
                            price_groups[order['price']] += order['size']
                    except Exception as e:
                        problematic_exchtime = order['exchtime']
                        print(f"Exception while processing sell order: {order}, Exception: {e}")
                top_levels = heapq.nsmallest(n, price_groups.items(), key=lambda x: x[0])

            elif side == 'buy':
                for ref, order in self.buy_orders.items():
                    try:
                        order_time = arrow.get(order['exchtime'], 'YYYY-MM-DD HH:mm:ss.SSSSSSSSS')
                        if ref not in self.removed_orders and (
                                thirty_minutes_ago is None or order_time >= thirty_minutes_ago):
                            price_groups[order['price']] += order['size']
                    except Exception as e:
                        problematic_exchtime = order['exchtime']
                        print(f"Exception while processing buy order: {order}, Exception: {e}")
                top_levels = heapq.nlargest(n, price_groups.items(), key=lambda x: x[0])

        except KeyError as e:
            problematic_exchtime = e.args[0]
            print(f"KeyError: {e}")

        return top_levels

    def vwap(self):
        total_value = sum(float(price) * float(size) for _, price, size in self.trades)
        total_volume = self.total_trade_volume()
        if total_volume == 0:
            return 0  # Avoid division by zero
        return total_value / total_volume

def read_orderbook_data(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            yield row

def compute_metrics(order_book, input_timestamp=None):
    thirty_minutes_ago = None
    if input_timestamp:
        thirty_minutes_ago = arrow.get(input_timestamp, 'YYYY-MM-DD HH:mm:ss.SSSSSSSSS').shift(minutes=-30)

    print(f"Metrics for the period ending at {input_timestamp}")
    print("Buy Qty")
    top_price = order_book.top_price_levels('buy', thirty_minutes_ago=thirty_minutes_ago)
    print(top_price)
    print("Sell Qty")
    top_sell = order_book.top_price_levels('sell', thirty_minutes_ago=thirty_minutes_ago)
    print(top_sell)
    print("Total Trade Volume:")
    total_trade_vol = order_book.total_trade_volume()
    print(total_trade_vol)
    print("VWAP:")
    vol_weighted_av_price = order_book.vwap()
    print(vol_weighted_av_price)

    return top_price, top_sell, total_trade_vol, vol_weighted_av_price
def main(input_csv=None):
    order_book = OrderBook()

    if input_csv:
        data_file = input_csv
    else:
        data_file = "csv path here"

    last_processed_time = None
    last_processed_date = None

    for message in read_orderbook_data(data_file):
        localtime, exchtime, msgtype, symbol, exch, price, size, side, ref, oldref, mpid, esd, platform = message

        # Check if it's a header row
        if exchtime == 'exchtime':
            continue

        order_book.process_message(message)

        current_time = arrow.get(exchtime, 'YYYY-MM-DD HH:mm:ss.SSSSSSSSS').timestamp()

        # Initialize start_time if not set
        if order_book.start_time is None:
            order_book.start_time = current_time

        # Check if it's been 30 minutes since last processed time or if the date has changed
        order_date = arrow.get(exchtime, 'YYYY-MM-DD').date()
        if last_processed_date is None or order_date != last_processed_date or current_time - last_processed_time >= 30 * 60:
            compute_metrics(order_book, input_timestamp=exchtime)
            # Get top buy and top sell from top_price_levels method
            last_processed_time = current_time
            last_processed_date = order_date

##################################
#if __name__ == "__main__":
#    main()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()