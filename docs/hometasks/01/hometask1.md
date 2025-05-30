Для выполнения первой домашней работы сформулируем цели антифрод-системы и проанализирует особенности проекта.

## Постановка задачи

**Бюджет:** 10 млн. рублей
**Сроки:** прототип через 3 месяца, полгода на окончательную реализации в случае удачного прототипа
**Целевые показатели и общие сведения:**

* 2% операций – мошеннические
* Общая ежемесячная потеря денежных средств не должна превышать 500 000 рублей
* Доля ошибок первого рода не должна превышать 5%

**Нагрузка:** в среднем 50 транзакций в секунду, максимальная - 400. **Прочие требования:**

* Использование облачной инфраструктуры
* Обеспечить безопасность при работе с чувствительными данными

### Цели проектируемой anti-fraud системы

Необходимо разработать anti-fraud систему, удовлетворяющую приведённым выше требованиям. Разработанную систему развернуть в качестве сервиса с использованием облачной инфраструктуры. Предусмотреть предварительную “очистку данных” - на этапе запроса к БД (на этапе обучения) либо запроса клиентского приложения к реализованному сервису по разработанному API (на этапе эксплуатации) чувствительные данные должны проходить предварительное обезличивание и поступать в систему в обезличенном виде (без явного содержания конфиденциальной информации о владельце транзакции). При ухудшении работы модели сервис должен оповещать клиентскую сторону и приостанавливать работу - предусмотреть автоматическое отключение (либо откат к предыдущей модели в случае, если таковая существует и продолжает удовлетворять требованиям на изменившимся данным). Разработать работающий прототип в течении 3 месяцев, доработать систему с учётом соответствия требованиям нагрузки и точности до полноценной эксплуатации за полгода. Предусмотреть автоматизацию всех процессов эксплуатации модели и безотказной работы сервиса.

### Метрика машинного обучения

Целесообразнее в данном случае использовать f-меру (параметр бета будет подбирать дополнительно): это позволит участь как долю ошибок первого рода (и задать на неё соответствующее ограничение), так и долю ошибок второго рода, так как в текущей постановке задачи необходимо соблюсти удовлетворяющий баланс между точностью и полнотой.

### Особенности проекта

[Contribution guidelines for this project](docs/hometasks/01/canvas.png)

### Kanban & Roadmap

Реализуемая система декомпозирована на высокоуровневые задачи в соответствии с требованиями по функциональности и срокам. Доску можно просматривать как в режиме Kanban, так и в Roadmap варианте с более нагрядной привязкой ко времени.

[Roadmap Link](https://github.com/users/brusia/projects/1/views/1?layout=roadmap)