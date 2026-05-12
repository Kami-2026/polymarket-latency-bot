async def execute_trade(token_id, direction, price, size_dollars):
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType, Side

        pk       = os.getenv("PK")
        host     = "https://clob.polymarket.com"
        chain_id = 137

        client = ClobClient(host, key=pk, chain_id=chain_id)
        client.set_api_creds(client.create_or_derive_api_creds())

        order_side  = Side.BUY
        order_price = round(price, 2)
        order_size  = round(size_dollars / price, 2)

        order = client.create_order(OrderArgs(
            token_id=token_id,
            price=order_price,
            size=order_size,
            side=order_side,
            order_type=OrderType.FOK
        ))
        resp = client.post_order(order, OrderType.FOK)
        plog(f"✅ ORDRE RÉEL | {direction} | "
             f"prix: {order_price} | size: {order_size} | "
             f"resp: {resp}")
        return resp

    except Exception as e:
        plog(f"⚠️ execute_trade: {e}")
        return None
