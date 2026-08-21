from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from quant_platform.config.loaders import (
    load_yaml,
    optional_mapping,
    require_mapping,
)


@dataclass(frozen=True)
class TechnicalConfig:
    backend: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class NormalizationConfig:
    winsorize_lower: float
    winsorize_upper: float
    min_cross_section_count: int


@dataclass(frozen=True)
class ResearchPanelConfig:
    factor_set: str
    label_set: str
    default_universe_name: str
    price_dwd_root: Path
    universe_membership_root: Path
    market_context_symbol_path: Path
    market_context_price_root: Path
    market_context_feature_root: Path
    feature_output_root: Path
    label_output_root: Path
    panel_output_root: Path
    feature_scope: dict[str, Any]
    rolling_windows: dict[str, list[int]]
    label_horizons: tuple[int, ...]
    technical: TechnicalConfig
    normalization: NormalizationConfig
    composites: dict[str, Any]


@dataclass(frozen=True)
class MarketContextSymbolSpec:
    context_group: str
    ticker: str
    is_required: bool
    is_primary_benchmark: bool


@dataclass(frozen=True)
class MarketContextConfig:
    context_set: str
    source: str
    dataset_name: str
    dim_security_path: Path
    symbol_output_path: Path
    symbols: tuple[MarketContextSymbolSpec, ...]


def _as_path(value: Any) -> Path:
    return Path(str(value).strip())


def _int_list(
    values: Any,
    *,
    field_name: str,
) -> list[int]:
    if not isinstance(values, list):
        raise ValueError(f"{field_name} must be a list")

    output = [int(value) for value in values]

    if any(value <= 0 for value in output):
        raise ValueError(f"{field_name} must contain positive integers")

    return output


def load_research_panel_config(
    config_path: str | Path,
) -> ResearchPanelConfig:
    data = load_yaml(Path(config_path))

    root = require_mapping(
        data,
        "research_panel",
        context="config",
    )

    rolling_raw = optional_mapping(
        root,
        "rolling_windows",
        context="research_panel",
    )
    labels_raw = optional_mapping(
        root,
        "labels",
        context="research_panel",
    )
    technical_raw = optional_mapping(
        root,
        "technical",
        context="research_panel",
    )
    normalization_raw = optional_mapping(
        root,
        "normalization",
        context="research_panel",
    )
    feature_scope = optional_mapping(
        root,
        "feature_scope",
        context="research_panel",
    )
    composites = optional_mapping(
        root,
        "composites",
        context="research_panel",
    )

    factor_set = str(root.get("factor_set", "core_v1")).strip()
    label_set = str(root.get("label_set", "core_v1")).strip()
    default_universe_name = str(
        root.get("default_universe_name", "us_liquid_500")
    ).strip()

    if not factor_set:
        raise ValueError("research_panel.factor_set must not be empty")

    if not label_set:
        raise ValueError("research_panel.label_set must not be empty")

    if not default_universe_name:
        raise ValueError(
            "research_panel.default_universe_name must not be empty"
        )

    label_horizons = tuple(
        _int_list(
            labels_raw.get("horizons", [1, 5, 21]),
            field_name="research_panel.labels.horizons",
        )
    )

    rolling_windows = {
        "returns": _int_list(
            rolling_raw.get("returns", [1, 5, 21, 63, 126, 252]),
            field_name="research_panel.rolling_windows.returns",
        ),
        "return_lag_multiples": _int_list(
            rolling_raw.get("return_lag_multiples", [1, 2, 3]),
            field_name="research_panel.rolling_windows.return_lag_multiples",
        ),
        "volatility": _int_list(
            rolling_raw.get("volatility", [21, 63, 126]),
            field_name="research_panel.rolling_windows.volatility",
        ),
        "dollar_volume": _int_list(
            rolling_raw.get("dollar_volume", [3, 20, 60]),
            field_name="research_panel.rolling_windows.dollar_volume",
        ),
        "price_position": _int_list(
            rolling_raw.get("price_position", [252]),
            field_name="research_panel.rolling_windows.price_position",
        ),
    }

    backend = str(technical_raw.get("backend", "talib")).strip().lower()

    if backend != "talib":
        raise ValueError("Only technical.backend='talib' is supported for now")

    winsorize_lower = float(
        normalization_raw.get("winsorize_lower", 0.01)
    )
    winsorize_upper = float(
        normalization_raw.get("winsorize_upper", 0.99)
    )

    if not 0 <= winsorize_lower < winsorize_upper <= 1:
        raise ValueError(
            "winsorize bounds must satisfy 0 <= lower < upper <= 1"
        )

    min_cross_section_count = int(
        normalization_raw.get("min_cross_section_count", 50)
    )

    if min_cross_section_count <= 0:
        raise ValueError("min_cross_section_count must be positive")

    return ResearchPanelConfig(
        factor_set=factor_set,
        label_set=label_set,
        default_universe_name=default_universe_name,
        price_dwd_root=_as_path(root["price_dwd_root"]),
        universe_membership_root=_as_path(root["universe_membership_root"]),
        market_context_symbol_path=_as_path(root["market_context_symbol_path"]),
        market_context_price_root=_as_path(root["market_context_price_root"]),
        market_context_feature_root=_as_path(root["market_context_feature_root"]),
        feature_output_root=_as_path(root["feature_output_root"]),
        label_output_root=_as_path(root["label_output_root"]),
        panel_output_root=_as_path(root["panel_output_root"]),
        feature_scope=feature_scope,
        rolling_windows=rolling_windows,
        label_horizons=label_horizons,
        technical=TechnicalConfig(
            backend=backend,
            raw=dict(technical_raw),
        ),
        normalization=NormalizationConfig(
            winsorize_lower=winsorize_lower,
            winsorize_upper=winsorize_upper,
            min_cross_section_count=min_cross_section_count,
        ),
        composites=dict(composites),
    )


def load_market_context_config(
    config_path: str | Path,
) -> MarketContextConfig:
    data = load_yaml(Path(config_path))

    root = require_mapping(
        data,
        "market_context",
        context="config",
    )

    context_set = str(root.get("context_set", "core_v1")).strip()
    source = str(root.get("source", "tiingo")).strip()
    dataset_name = str(
        root.get("dataset_name", "market_context_price_daily")
    ).strip()

    if not context_set:
        raise ValueError("market_context.context_set must not be empty")

    primary_benchmarks = {
        str(value).strip().upper()
        for value in root.get("primary_benchmarks", [])
        if str(value).strip()
    }

    symbols_raw = require_mapping(
        root,
        "symbols",
        context="market_context",
    )

    specs: list[MarketContextSymbolSpec] = []
    seen: set[str] = set()

    for context_group, tickers in symbols_raw.items():
        if not isinstance(tickers, list):
            raise ValueError(
                f"market_context.symbols.{context_group} must be a list"
            )

        for ticker_raw in tickers:
            ticker = str(ticker_raw).strip().upper()

            if not ticker:
                continue

            if ticker in seen:
                raise ValueError(
                    f"Duplicate market context ticker: {ticker}"
                )

            seen.add(ticker)

            specs.append(
                MarketContextSymbolSpec(
                    context_group=str(context_group).strip(),
                    ticker=ticker,
                    is_required=True,
                    is_primary_benchmark=ticker in primary_benchmarks,
                )
            )

    if not specs:
        raise ValueError("market_context.symbols must not be empty")

    return MarketContextConfig(
        context_set=context_set,
        source=source,
        dataset_name=dataset_name,
        dim_security_path=_as_path(root["dim_security_path"]),
        symbol_output_path=_as_path(root["symbol_output_path"]),
        symbols=tuple(specs),
    )