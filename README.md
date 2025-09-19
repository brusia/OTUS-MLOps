# OTUS-MLOps

Данный репозиторий создан для выполнения домашних заданий по курсу MLOps платформы обучения OTUS.

<!-- Start of Hometask 4 block -->

## Задание 4

### Общие положения

В рамках выполнения задания 4 при использовании скриптов terraform на отдельной виртуальной машине был развёрнут feature-store (feast). Использована базовая модульная инфраструктура для создания виртуальной машины и сети.

В качестве предустановочного скрипта в момент создания VM используется infra/main/scripts/setup_feature_store.sh, который скачивает на виртуальный сервер проект (github) -- передаётся при помощи аргумента -- устанавливает необходимые зависимости и разворачивает feature-store.

Репозиторий исходно получен запуском feast init c внесением незначительны изменений, а именно:

- добавлен дополнительный feature_view с метриками оценки состояния водителя
- добавлен on-demand feature_view для вычисления взвешенной оценки качества вождения (веса передаются в запросе)
- добавлен feature_service для работы с вычислением взвешенной оценки
- добавлен дополнительный источник данных (содержащий, помимо вышеупомянутых метрик, также произведение трёх исходных показателей -- productivity)
- добавлен feature_view для работы с productivity

### Скриншоты

feast ui в браузере:

- [Источники данных](docs/hometasks/04/data_sources.png)
- [Добавленный feature service](docs/hometasks/04/feat_service_linear.png)
- [Доступные feature-views](docs/hometasks/04/feature_views.png)
- [Доступные признаки](docs/hometasks/04/features.png)

### Пример

В качестве примера взаимодействия с feature-store реализован скрипт feature-store/otus_mlops_feature_store/feature_repo/test_workflow.py

[Пример вывода скрипта](docs/hometasks/04/result.txt)

### Примечание

Образец (автоматически генерируемый feast-ом проект) предлагает поместить примеры взаимодействия с feature-store в feature_repo/test_workflow.py, по этой причине и был выбран данный путь.

Файл infra/main/managing_proxy/main.tf изменён в соответствии с текущим заданием (и в дальнейшем будем возвращён к предыдущей реализации).

<!-- End of Hometask 4 block. -->

## Oбщие положения

Для экономии места выполненные (и принятые) задания перемещены из главного файла README.md текущего репозитория и скрыты в соответствующих каталогах (docs/hometasks/<task_number>/hometask<task_number>.md)

Актуальное задание (для проверки) будет размещено в текущей версии README.md
Список предыдущих заданий

- [Hometask 1](docs/hometasks//01/hometask1.md)
- [Hometask 2](docs/hometasks/02/hometask2.md)
- [Hometask 3](docs/hometasks/03/hometask3.md)
- [Hometask 4](docs/hometasks/04/hometask04.md)
- [Hometask 5](docs/hometasks/05/hometask5.md)
- [Hometask 6](docs/hometasks/06/hometask06.md)
- [Hometask 7](docs/hometasks/07/hometask07.md)

Статусы задач поддерживаются в актуальном состоянии на протяжении всей работы. Kandan-доска доступна по ссылке:

[Tasks View](https://github.com/users/brusia/projects/1/views/1)
