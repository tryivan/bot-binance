"""Registro de operações encerradas em CSV — PR 1.12.

Para cada posição encerrada, busca os trades reais via fetch_my_trades,
agrega entrada/saída, infere motivo de saída (TP, SL, outro) e escreve
uma linha no CSV de relatórios. Tolerante a falhas: qualquer erro
durante a coleta loga WARNING e retorna sem escrever (linha parcial
poluiria agregações futuras do dashboard).
"""

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import ccxt.async_support as ccxt


TRADE_CSV_COLUMNS = [
    "symbol",
    "side",
    "opened_at",
    "closed_at",
    "duration_seconds",
    "entry_price",
    "exit_price",
    "quantity",
    "leverage",
    "stop_loss_price",
    "take_profit_price",
    "exit_reason",
    "realized_pnl_usdt",
    "realized_pnl_percent",
    "fees_usdt",
]


ExitReason = Literal["tp", "sl", "other"]


class TradeReporter:
    """Persiste resumos de operações encerradas em CSV.

    Não interfere no fluxo de trading: se a coleta de trades falhar,
    a operação não é registrada e o bot prossegue.
    """

    _FETCH_TRADES_LIMIT = 100
    # Tolerância para classificar exit_reason via preço quando o match
    # por orderId não está disponível. Margem de slippage em pares voláteis.
    _PRICE_TOLERANCE_PCT = 0.2
    # Buffer pós-encerramento para descartar trades de uma eventual nova
    # posição que tenha aberto logo em seguida no mesmo símbolo.
    _CLOSE_BUFFER_SECONDS = 5

    def __init__(
        self,
        exchange: ccxt.Exchange,
        logger: logging.Logger,
        csv_path: Path,
    ) -> None:
        self._exchange = exchange
        self._log = logger
        self._csv_path = Path(csv_path)

    async def record_closed_trade(
        self,
        symbol: str,
        side: Literal["buy", "sell"],
        opened_at: datetime,
        entry_price: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
        leverage: int,
        sl_order_id: Optional[str] = None,
        tp_order_id: Optional[str] = None,
    ) -> None:
        """Coleta trades, agrega e escreve linha no CSV. Tolerante a falha."""
        closed_at = datetime.now(timezone.utc)
        opened_at_utc = self._ensure_utc(opened_at)

        try:
            trades = await self._fetch_trades(symbol, opened_at_utc, closed_at)
        except Exception as exc:
            self._log.warning(
                f"[{symbol}] Falha ao buscar trades para registro: {exc}"
            )
            return

        if not trades:
            self._log.warning(
                f"[{symbol}] Sem trades retornados desde {opened_at_utc.isoformat()}. "
                f"Operação não registrada."
            )
            return

        entry_fills, exit_fills = self._split_entry_exit(trades, side)

        if not entry_fills or not exit_fills:
            self._log.warning(
                f"[{symbol}] Trades insuficientes para reconstruir operação "
                f"(entradas={len(entry_fills)}, saídas={len(exit_fills)}). "
                f"Operação não registrada."
            )
            return

        entry_vwap, entry_qty, entry_fee = self._aggregate_fills(entry_fills)
        exit_vwap, exit_qty, exit_fee = self._aggregate_fills(exit_fills)
        realized_pnl = self._sum_realized_pnl(exit_fills)
        exit_reason = self._infer_exit_reason(
            exit_fills, sl_price, tp_price, sl_order_id, tp_order_id
        )

        last_exit_ms = max(int(t.get("timestamp") or 0) for t in exit_fills)
        if last_exit_ms > 0:
            closed_at = datetime.fromtimestamp(last_exit_ms / 1000, tz=timezone.utc)

        duration_seconds = max(int((closed_at - opened_at_utc).total_seconds()), 0)
        notional = entry_vwap * entry_qty
        margin = notional / leverage if leverage > 0 else notional
        pnl_percent = (realized_pnl / margin * 100) if margin > 0 else 0.0

        row = {
            "symbol": symbol,
            "side": side,
            "opened_at": opened_at_utc.isoformat().replace("+00:00", "Z"),
            "closed_at": closed_at.isoformat().replace("+00:00", "Z"),
            "duration_seconds": duration_seconds,
            "entry_price": f"{entry_vwap:.8f}",
            "exit_price": f"{exit_vwap:.8f}",
            "quantity": f"{exit_qty:.8f}",
            "leverage": leverage,
            "stop_loss_price": f"{sl_price:.8f}" if sl_price is not None else "",
            "take_profit_price": f"{tp_price:.8f}" if tp_price is not None else "",
            "exit_reason": exit_reason,
            "realized_pnl_usdt": f"{realized_pnl:.8f}",
            "realized_pnl_percent": f"{pnl_percent:.4f}",
            "fees_usdt": f"{(entry_fee + exit_fee):.8f}",
        }

        try:
            self._write_row(row)
        except Exception as exc:
            self._log.warning(
                f"[{symbol}] Falha ao gravar linha no CSV de trades: {exc}"
            )
            return

        self._log.info(
            f"[{symbol}] Operação registrada em {self._csv_path.name}: "
            f"reason={exit_reason} pnl={realized_pnl:.4f} USDT ({pnl_percent:.2f}%)"
        )

    # ------------------------------------------------------------------
    # Coleta e agregação
    # ------------------------------------------------------------------

    async def _fetch_trades(
        self, symbol: str, opened_at: datetime, closed_at: datetime
    ) -> List[Dict[str, Any]]:
        """Busca trades do símbolo desde opened_at, limitado por janela temporal."""
        since_ms = int(opened_at.timestamp() * 1000)
        cutoff_ms = int(closed_at.timestamp() * 1000) + (
            self._CLOSE_BUFFER_SECONDS * 1000
        )

        trades = await self._exchange.fetch_my_trades(
            symbol, since_ms, self._FETCH_TRADES_LIMIT
        ) or []

        bounded: List[Dict[str, Any]] = []
        for t in trades:
            ts = int(t.get("timestamp") or 0)
            if ts and ts <= cutoff_ms:
                bounded.append(t)
        return bounded

    def _split_entry_exit(
        self, trades: List[Dict[str, Any]], position_side: Literal["buy", "sell"]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Separa trades em fills de entrada (mesmo lado) e saída (lado oposto)."""
        entries: List[Dict[str, Any]] = []
        exits: List[Dict[str, Any]] = []
        for t in trades:
            trade_side = str(t.get("side") or "").lower()
            if trade_side == position_side:
                entries.append(t)
            elif trade_side in ("buy", "sell"):
                exits.append(t)
        return entries, exits

    def _aggregate_fills(
        self, fills: List[Dict[str, Any]]
    ) -> Tuple[float, float, float]:
        """Retorna (vwap_price, total_qty, total_fee_usdt) para uma lista de fills."""
        total_qty = 0.0
        total_notional = 0.0
        total_fee = 0.0
        for fill in fills:
            qty = float(fill.get("amount") or 0)
            price = float(fill.get("price") or 0)
            total_qty += qty
            total_notional += qty * price
            total_fee += self._extract_fee_usdt(fill)
        vwap = total_notional / total_qty if total_qty > 0 else 0.0
        return vwap, total_qty, total_fee

    @staticmethod
    def _extract_fee_usdt(trade: Dict[str, Any]) -> float:
        """Extrai fee em USDT (Binance Futures USDT-M paga em USDT)."""
        fee = trade.get("fee") or {}
        cost = fee.get("cost")
        try:
            return float(cost) if cost is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _sum_realized_pnl(exit_fills: List[Dict[str, Any]]) -> float:
        """Soma realizedPnl dos trades de saída (Binance retorna em info)."""
        total = 0.0
        for fill in exit_fills:
            info = fill.get("info") or {}
            raw = info.get("realizedPnl")
            if raw is None:
                continue
            try:
                total += float(raw)
            except (TypeError, ValueError):
                continue
        return total

    # ------------------------------------------------------------------
    # Classificação do motivo de saída
    # ------------------------------------------------------------------

    def _infer_exit_reason(
        self,
        exit_fills: List[Dict[str, Any]],
        sl_price: Optional[float],
        tp_price: Optional[float],
        sl_order_id: Optional[str],
        tp_order_id: Optional[str],
    ) -> ExitReason:
        """Classifica saída como tp, sl ou other.

        Estratégia primária: match por orderId (campo `order` do trade) contra
        os ids das ordens SL/TP que abrimos. Estratégia de fallback: comparar
        VWAP de saída contra stopPrice de SL/TP com tolerância configurável.
        """
        if sl_order_id or tp_order_id:
            order_ids = {str(t.get("order")) for t in exit_fills if t.get("order")}
            if sl_order_id and str(sl_order_id) in order_ids:
                return "sl"
            if tp_order_id and str(tp_order_id) in order_ids:
                return "tp"

        exit_vwap, _, _ = self._aggregate_fills(exit_fills)
        if exit_vwap <= 0:
            return "other"

        if tp_price is not None and self._within_tolerance(exit_vwap, tp_price):
            return "tp"
        if sl_price is not None and self._within_tolerance(exit_vwap, sl_price):
            return "sl"
        return "other"

    def _within_tolerance(self, price: float, target: float) -> bool:
        if target <= 0:
            return False
        diff_pct = abs(price - target) / target * 100
        return diff_pct <= self._PRICE_TOLERANCE_PCT

    # ------------------------------------------------------------------
    # Escrita em CSV
    # ------------------------------------------------------------------

    def _write_row(self, row: Dict[str, Any]) -> None:
        """Append; cria diretório e header se ainda não existem."""
        self._csv_path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not self._csv_path.exists()

        with self._csv_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TRADE_CSV_COLUMNS)
            if write_header:
                writer.writeheader()
            writer.writerow(row)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ensure_utc(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
