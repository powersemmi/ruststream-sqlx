---
sidebar_position: 3
description: "Переобработка задачи сразу или позже с собственной отсрочкой обработчика, предел попыток и перемещение того, что не обрабатывается, в группу dead letter."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Переобработка и dead letter

Это руководство переобрабатывает задачу сразу или позже, ограничивает число её попыток и откладывает то, что раз за разом не обрабатывается, в [группу dead letter](../components/settlement/dead-letter.md) той же таблицы.

Переобработка здесь - это оператор над строкой задачи.
Строка сохраняет свой id и своё место в таблице, засчитывает ещё одну попытку, а задержка - это время в одном из её столбцов.
Ничего не хранится только в процессе: задача, которая ждёт переобработки, переживает перезапуск.

Пример - сервис рассылки, который отправляет письма через почтового провайдера.

## Таблица {/* #the-table */}

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/retries.rs#schema"
```

`name` - это [группа](../components/tables/roles.md), в которой ждёт строка: подписка на `emails` читает строки, у которых `name` равно `emails`.
`attempt` считает доставки строки, включая первую.
`retry_after` - время, когда строка становится готовой к захвату; значение по умолчанию делает новую строку готовой сразу.

## Подписчик {/* #the-subscriber */}

Провайдер отказывает в отправке письма тремя способами, и каждому нужен свой исход.
`Busy` значит, что его лимит запросов исчерпан: попытка прямо сейчас снова получит отказ, поэтому письмо ждёт.
`Unavailable` значит, что провайдер не ответил: `retry` пробует снова сразу.
`Rejected` значит, что почтового ящика не существует, и новая попытка получит тот же ответ, поэтому письмо отбрасывается.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#send"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#send"
```

</TabItem>
</Tabs>

Отсрочку выбирает обработчик.
Он читает [попытку](../components/context/attempt.md) доставки и с каждой попыткой просит на секунду больше: [`retry_after(d)`](../components/settlement/retry-after.md) записывает «сейчас» плюс `d` в столбец `retry_after` строки, и до этого времени строку не берёт ни один захват.
Таблица без этого столбца возвращает строку сразу, а рантайм пишет предупреждение в журнал.
[`retry()`](../components/settlement/index.md) сразу возвращает строку в очередь; её следующая доставка читает попытку на единицу больше.
`drop()` завершает строку так же, как подтверждение, поэтому отклонённое письмо уходит из таблицы.
Строка, полезная нагрузка которой не декодируется, тратит свои попытки, как при любой другой ошибке ([Декодирование](../components/messages/decoding.md)).

## Предел и группа dead letter {/* #the-cap-and-the-dead-letter-group */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) читает `attempt` строки.
Пятая неудачная доставка не переобрабатывается: [`dead_letter`](../components/settlement/dead-letter.md) перемещает строку в группу `emails.dead` с полезной нагрузкой и попыткой в том виде, в каком они были.
Отложенная переобработка засчитывается в предел так же, как немедленная.
Два шага задаются вместе: точка монтирования, которая объявляет один из них без другого, останавливает сервис при старте.
Таблица без столбца группы принимает в `dead_letter` имя таблицы, и строка перемещается в эту таблицу с теми же столбцами.

## Чтение dead letter {/* #reading-the-dead-letters */}

Группа dead letter - обычная очередь таблицы, и `review` читает её, как любую другую.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`review` пишет письмо в журнал, чтобы его посмотрел человек, и подтверждает его.
Сервис, который хранит dead letter для оператора, не монтирует подписку на эту группу, и строки ждут в таблице.
Чтобы отправить их снова, достаточно одного собственного оператора сервиса, который возвращает строки и сбрасывает их счётчик:

```sql
UPDATE mail_jobs SET name = 'emails', attempt = 1, locked_until = NULL WHERE name = 'emails.dead';
```

В таблице с [группами FIFO](../components/subscriptions/fifo.md) `retry()` оставляет строку в голове её группы, а `retry_after(d)` пропускает вперёд строки, готовые раньше ([Порядок](ordering.md)).

## Тестирование {/* #testing */}

Тесты в файле примера публикуют письмо через обвязку так, как его записывает собственный `Publish` сервиса.
Письмо на отклонённый адрес отбрасывается сразу, и [`TestApp::start`](../components/testing/index.md) показывает это, когда публикация возвращает управление.
Письму без ответа и занятому провайдеру нужно, чтобы прошло время: отложенная строка и dead letter возвращаются при следующем опросе подписок.
Эти тесты работают на реальных часах через [`TestApp::start_live`](../components/testing/live.md) с отсрочкой в одну секунду, и `tb.advance` выжидает её.
Работу с обвязкой описывает руководство [Тестирование сервиса на SQL](testing.md).
