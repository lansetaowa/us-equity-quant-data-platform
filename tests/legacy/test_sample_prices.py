from scripts.legacy.create_sample_prices import build_sample_prices


def test_sample_prices_schema() -> None:
    df = build_sample_prices()

    expected_columns = {
        "security_id",
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
        "load_id",
    }

    assert expected_columns.issubset(set(df.columns))


def test_sample_prices_has_unique_security_date() -> None:
    df = build_sample_prices()

    duplicated = df.duplicated(["security_id", "date"]).sum()

    assert duplicated == 0


def test_sample_prices_values_are_valid() -> None:
    df = build_sample_prices()

    assert (df["close"] > 0).all()
    assert (df["high"] >= df["low"]).all()
    assert (df["volume"] >= 0).all()