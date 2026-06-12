# scanners_warroom_insight.py - WARROOM INSIGHT

import logging

from utils import fmt_price
from smc_engine import get_warroom_insight, detect_trendline, find_fib_levels, detect_liquidity_hunt, get_volume_poc
from scanners_cross_tag import _cross_tag

logger = logging.getLogger(__name__)

def build_warroom_insight_block(coin, direction):
    """Bangun blok insight untuk warroom alert (trendline, fib, hunt, poc, killzone)"""
    try:
        insight = get_warroom_insight(coin)
    except:
        insight = None
    
    tl_lines, fib_info, hunt_info, poc_info, killzone_info = [], "", "", "", ""
    
    if insight:
        tl_4h = insight.get("tl_4h_long" if direction == "LONG" else "tl_4h_short", {})
        tl_1h = insight.get("tl_1h_long" if direction == "LONG" else "tl_1h_short", {})
        
        if tl_4h.get("has_trendline") and not tl_4h.get("is_broken"):
            tl_lines.append(f"4H {tl_4h.get('type','TL')} @ {fmt_price(tl_4h.get('price',0))} (x{tl_4h.get('touches',0)})")
        if tl_1h.get("has_trendline") and not tl_1h.get("is_broken"):
            tl_lines.append(f"1H {tl_1h.get('type','TL')} @ {fmt_price(tl_1h.get('price',0))} (x{tl_1h.get('touches',0)})")
        
        if insight.get("nearest_fib"):
            lvl, px = insight["nearest_fib"]
            fib_info = f"📐 FIB {lvl} @ {fmt_price(px)}"
        
        hunt_key = "hunt_long" if direction == "LONG" else "hunt_short"
        hunt = insight.get(hunt_key, {})
        if hunt.get("is_hunting") and hunt.get("confidence", 0) >= 50:
            hunt_info = f"🚨 LIQ HUNT {hunt['hunt_type']} depth={hunt['hunt_depth']:.1f}x ATR"
        
        if insight.get("poc") and insight.get("poc_dist", 999) < 1.0:
            poc_info = f"📊 POC @ {fmt_price(insight['poc']['price'])} (dist {insight['poc_dist']:.2f}%)"
        
        if insight.get("killzone_mins", 999) <= 60:
            killzone_info = f"⏰ KILLZONE: {insight['killzone_name']} in {insight['killzone_mins']}m"
    
    extra_sections = []
    if tl_lines:
        extra_sections.append("📉 TL: " + "  |  ".join(tl_lines))
    if fib_info:
        extra_sections.append(fib_info)
    if hunt_info:
        extra_sections.append(hunt_info)
    if poc_info:
        extra_sections.append(poc_info)
    if killzone_info:
        extra_sections.append(killzone_info)
    
    extra_block = ("\n" + "\n".join(extra_sections)) if extra_sections else ""
    
    return extra_block
