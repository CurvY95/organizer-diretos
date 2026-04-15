# Organizer Diretos (Encomendas)

Web app para enviar um arquivo com encomendas + preços e obter:

- total a pagar por pessoa
- lista de encomendas por pessoa
- texto final para copiar e enviar

## Como rodar

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Formatos aceitos

### Opção A (recomendado): Excel `.xlsx` com 2 abas

- **Aba 1 (encomendas)**: `Cliente`, `Produto`, `Quantidade`
- **Aba 2 (preços)**: `Produto`, `Preco`

Os nomes das abas podem variar: o app tenta detectar automaticamente, e também permite você escolher.

### Opção B: dois CSVs

- CSV de encomendas com `Cliente`, `Produto`, `Quantidade`
- CSV de preços com `Produto`, `Preco`

## Observações

- `Quantidade` pode ser inteira ou decimal.
- `Preco` pode vir como `10.5` ou `10,5` (o app normaliza).
