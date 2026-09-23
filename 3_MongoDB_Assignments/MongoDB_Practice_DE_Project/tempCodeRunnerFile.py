except (FileNotFoundError, KeyError, json.JSONDecodeError):
    # If the checkpoint file doesn't exist or is invalid,
    # start processing from the beginning.
    last_order_id = 0

print(f"Last processed order ID: {last_order_id}")