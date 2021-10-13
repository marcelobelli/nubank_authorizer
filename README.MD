# Code Challenge Nubank

## Pré-requisitos
* python >= 3.10.0

## Instalando as dependências

Caso queira apenas rodar o script: `pip install -r requirements.txt`

Para rodar a suíte de testes: `pip install -r requirements-dev.txt`

### Rodando a suíte de testes

`pytest ./tests/`

## Rodando o script

Basta executar:

`$ python authorize.py < operations`

Sendo `operations` o arquivo com as operações.


## Sobre o desafio

### Decisões técnicas e arquiteturais

Trabalhar com o estado da conta e das operações em memória inicialmente se mostrou
um desafio. Trabalhando com o python da maneira que estou acostumado era fácil esbarrar
em alguns _corner cases_, principalmente por conta da mutabilidade. No fim o código ficava
muito complexo e de difícil leitura. Foi quando eu tive a ideia de "imitar" alguns experimentos
que havia feito com Elixir onde a grande sacada era passar o estado do "sistema" entre as funções.

No fim acabei fazendo exatamente isso na minha solução, mas com algumas "licenças poéticas", me aproveitando da
mutabilidade onde tinha pleno controle do que estava ocorrendo.

O estado da conta é guardado na classe `AccountState` e dos processors que verificam as regras `high-frequency-small-interval` e
`doubled-transaction` nas classes `FrequencyState` e `RepeatedState` que são guardadas dentro do `AccountState`.

Cada transação é inputada na função `authorize_transaction` junto com o `AccountState`. A transação passará por todas as
regras e no fim o `authorize_transaction` irá entregar o `AccountState` e resultado da operação, que será enviado para 
o `stdout`. Após isso outra transação será inputada juntamente com o `AccountState` no `authorize_transaction` até que
todas tenham sido processadas.

###  Bibliotecas usadas

* **pydantic**: Facilita o uso de dataclasses, principalmente quando o valor inicial é um objeto mutável.
* **pendulum**: Torna muito mais fácil a vida de quem vai trabalhar com dates, datetimes e afins.