from threading import Thread
from queue import Queue
from tabulate import tabulate

import os

class L3OrderBookManager:
    def __init__(self):
        self.buys = dict() # {price: [order, order, ...]}
        self.sells = dict() # {price: [order, order, ...]}

        self.orders = dict() # {order_id: order}

        # Debugging
        self.deleted = set()
        self.inserted = set()

        self.best_buy_order = None
        self.best_sell_order = None
    
    def handle_event(self, lob_event):
        if lob_event['lob_action'] == 2:
            self._insert(lob_event)
        elif lob_event['lob_action'] == 3:
            self._delete(lob_event)
        elif lob_event['lob_action'] == 4:
            self._update(lob_event)

    def dump(self):
        nrows = 10
        headers = ["Price", "Size"]

        n_indexed = 0
        bids = []
        for price in sorted(self.buys.keys(), reverse=True):
            bids.append((price, self._get_level_size(price, self.buys)))
            n_indexed += 1
            if n_indexed == nrows:
                n_indexed = 0
                break

        asks = []
        for price in sorted(self.sells.keys()):
            asks.append((price, self._get_level_size(price, self.sells)))
            n_indexed += 1
            if n_indexed == nrows:
                n_indexed = 0
                break
        asks.reverse()

        print("ASKS")
        print(tabulate(asks[:nrows], headers=headers, tablefmt="fancy_grid"))

        print("BIDS")
        print(tabulate(bids[:nrows], headers=headers, tablefmt="fancy_grid"))

        print("\nBEST ASK: " + str(self.best_sell_order))
        print("\nBEST BID: " + str(self.best_buy_order))
    
    def _insert(self, lob_event):
        order_id = lob_event["order_id"]
        price = lob_event["price"]
        size = lob_event["size"]
        side = lob_event["side"]
        order_book = self.buys if side == 1 else self.sells
        if price not in order_book.keys():
            order_book[price] = [lob_event]
        else:
            order_book[price].append(lob_event)
        self.orders[order_id] = lob_event
        if not (self.best_buy_order and self.best_sell_order) or \
                (side == 1 and price >= self.best_buy_order["price"]) or \
                (side == 2 and price <= self.best_sell_order["price"]):
            self._update_best_levels(side)
        
        self.inserted.add(order_id)

    def _delete(self, lob_event):
        order_id = lob_event["order_id"]
        if order_id not in self.orders.keys():
            raise KeyError(f"order_id {order_id} not an active order.")
        order = self.orders[order_id]
        price = order["price"]
        side = order["side"]
        order_book = self.buys if side == 1 else self.sells
        if price not in order_book.keys():
            anti_order_book = self.sells if side == 1 else self.buys
            raise KeyError(f"Deleting order at price {price} when price is not in orderbook. \
                    Price in opposing orderbook: {price in anti_order_book}")
        self._pop_order(order_book, price, order_id)
        del self.orders[order_id]
        if (side == 1 and price == self.best_buy_order["price"]) or \
                (side == 2 and price == self.best_sell_order["price"]):
            self._update_best_levels(side)

        self.deleted.add(order_id)

    def _update(self, lob_event):
        order_id = lob_event["order_id"]
        if order_id not in self.orders.keys():
            raise KeyError(f"order_id {order_id} not an active order.\n \
                    order been deleted: {order_id in self.deleted}, order been added: {order_id in self.inserted}")
        order = self.orders[order_id]
        old_price = order["price"]
        order_book = self.buys if order["side"] == 1 else self.sells
        order = self._pop_order(order_book, order["price"], order_id)
        if order["side"] != lob_event["side"]:
            raise ValueError("Need to implement side updates")
        order["price"] = lob_event["price"]
        order["size"] = lob_event["size"]
        order["side"] = lob_event["side"]

        price = lob_event["price"]
        side = lob_event["side"]
        if price not in order_book.keys():
            order_book[price] = [order]
        else:
            order_book[price].append(order)
        self.orders[order_id] = order
        if (side == 1 and old_price == self.best_buy_order["price"]) or \
                (side == 2 and old_price == self.best_sell_order["price"]):
            self._update_best_levels(side)
    
    def _pop_order(self, book, price, order_id):
        for i in range(len(book[price])):
            order = book[price][i]
            if order_id == order["order_id"]:
                book[price].pop(i)
                if len(book[price]) == 0:
                    del book[price]
                return order
        raise KeyError(f"order_id {order_id} does not exist in the orderbook.")
    
    def _get_level_size(self, price, book):
        size = 0
        for i in range(len(book[price])):
            order = book[price][i]
            size += order["size"]
        if size == 0:
            raise ValueError(f"Empty price level {price}")
        return size

    def _update_best_levels(self, side):
        book = self.buys if side == 1 else self.sells
        best_price = max(book.keys()) if side == 1 else min(book.keys())
        best_size = self._get_level_size(best_price, book)

        if side == 1:
            self.best_buy_order = {"price": best_price, "size": best_size}
        else:
            self.best_sell_order = {"price": best_price, "size": best_size}

def main():
    """
    Simple tests for the L3OrderBookManager class
    """
    os.system("cls")
    print("------------ORDERBOOK TEST------------")
    order_book = L3OrderBookManager()

    # Testing insertion
    order1 = create_order(1, 100, 10, 1, 2)
    order2 = create_order(2, 100, 20, 1, 2)
    order3 = create_order(3, 100, 30, 1, 2)
    order4 = create_order(4, 100, 40, 1, 2)
    order5 = create_order(5, 100, 50, 1, 2)
    orders = [order1, order2, order3, order4, order5]
    for order in orders:
        order_book.handle_event(order)
    order_book.dump()

    # Testing deletion
    order = create_order(3, 0, 0, 1, 3)
    order_book.handle_event(order)
    order_book.dump()

    # Testing update
    order = create_order(1, 100, 40, 1, 4)
    order_book.handle_event(order)
    order_book.dump()

    # Testing empty level
    order = create_order(6, 200, 40, 1, 2)
    order_book.handle_event(order)
    order_book.dump()
    order = create_order(6, 50, 0, 1, 3)
    order_book.handle_event(order)
    order_book.dump()
    print("----------END ORDERBOOK TEST----------")

def create_order(order_id, price, size, side, lob_action):
    return {
        "order_id": order_id,
        "price": price,
        "size": size,
        "side": side,
        "lob_action": lob_action
    }

if __name__ == '__main__':
    main()