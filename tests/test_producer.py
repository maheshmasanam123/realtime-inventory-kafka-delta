from producer.wms_event_producer import make_event, EVENT_TYPES


def test_event_shape():
    e = make_event()
    assert set(e) >= {"event_id", "event_type", "warehouse_id", "sku", "qty_delta", "event_time"}
    assert e["event_type"] in EVENT_TYPES
    assert isinstance(e["qty_delta"], int)


def test_pick_is_negative():
    for _ in range(200):
        e = make_event()
        if e["event_type"] == "PICK":
            assert e["qty_delta"] < 0
