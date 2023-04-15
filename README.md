# Coding Sample

### Outline
 1. Create Dataset from crypto exchange apis
   * collect orderbook snapshots/executed trades
 2. Create a small model to predict short term volatitliy
   * maybe transformer or decision tree
 3. Run model in a live setting, do some nice visualizations

### Dataset
 * [x] Connect to websocket apis
 * [x] Subscribe to orderbook and trades feeds
 * [ ] Maintain orderbooks
 * [ ] Write orderbook and trades to repo (in this case probably csv)
   * collect data for kraken and coinbase
   * 1 second granularity
   * cap orderbook depth at 100
 * [ ] Collect data for coinmarketcap top10 non-stablecoins

### Model
 * [ ] Train a simple transformer model in colab on orderbook snapshots and trades.
 * [ ] Predict realized volatility
 * [ ] Evaluate on room mean square percentage error
 * [ ] Maybe create exchange and symbol embeddings

### Live Setting
 * [ ] 
 
