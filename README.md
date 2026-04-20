# Organizer Diretos

Web app em Streamlit para fechar encomendas de vendas em direto/live sales.

O fluxo principal é:

1. importar comentários/encomendas de `.csv` ou `.xlsx`;
2. ajustar quantidades, remover linhas e limpar comentários;
3. importar ou preencher preços por referência;
4. calcular totais por cliente;
5. gerar mensagens para copiar/enviar;
6. abrir chat/inbox/perfil do Facebook Business;
7. gerar etiquetas HTML 10x15 cm para impressão;
8. guardar sessões para continuar mais tarde.

## Como correr

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export ORGANIZER_USER="admin"
export ORGANIZER_PASS="trocar-esta-password"

streamlit run app.py
```

Também pode usar `AUTH_USER` e `AUTH_PASS` em `.streamlit/secrets.toml`.
Veja também `.env.example` para as variáveis suportadas.

## Testes

Os testes usam `unittest` e não exigem dependências extra além das dependências da app:

```bash
python3 -m unittest discover
```

Há ficheiros de exemplo em `examples/`:

- `examples/encomendas_comments.csv`
- `examples/precos.csv`

## Formatos aceites

### Encomendas / comments

CSV ou Excel com colunas equivalentes a:

- `Cliente`
- `Produto` ou `Referencia`
- `Quantidade`

Campos opcionais:

- `UserId`
- `ProfileId`
- `Hora`
- `Comentario`

A app aceita vários aliases, por exemplo `Nome`, `Ref`, `Qtd`, `Mensagem`, `Comment` e outros.

### Preços

CSV ou Excel com colunas equivalentes a:

- `Produto` ou `Referencia`
- `Preco`

`Preco` pode vir como `10.5`, `10,5` ou com separadores de milhares comuns.

## Persistência

Sem configuração extra, a app guarda dados locais em `saved/`:

- sessões JSON em `saved/sessions/`;
- estado local em `saved/organizer_state.json`;
- SQLite em `saved/organizer.db`.

Em produção, pode definir `DATABASE_URL` para usar Postgres/Supabase. Se for Postgres, a app usa o schema `organizer` por defeito, ou `DB_SCHEMA` se estiver configurado.

## Branding

Pode configurar:

- `BRAND_NAME`
- `BRAND_TAGLINE`
- `BRAND_PRIMARY`
- `BRAND_LOGO_PATH`

O logo também pode ficar em `assets/logo.png` ou `assets/logo.svg`.

## Segurança

O login atual é uma proteção simples por utilizador/password configurados no ambiente ou em `st.secrets`.
Para exposição pública com múltiplos utilizadores, use autenticação externa no serviço de deploy ou uma camada própria
com sessões, expiração e auditoria.
