

def save_leads_to_excel(self, leads: list[dict], filename: str = "leads_master.xlsx") -> None:
    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / "data"
    data_dir.mkdir(exist_ok=True)

    filepath = data_dir / filename

    # print(leads[0].keys())
    #
    # print(leads[0].get("website"), leads[0].get("primary_contact"))

    df_new = pd.DataFrame(leads)

    if not filepath.exists():
        # первый запуск — только новые данные
        df_all = df_new.copy()
    else:
        # читаем старые, дозаписываем новые со склейкой по строкам -> строки просто идут одна под другой
        df_old = pd.read_excel(filepath, dtype={"id": str})
        df_all = pd.concat([df_new, df_old], ignore_index=True)

    # убираем дубли по адресу
    df_all.drop_duplicates(subset=["address"], inplace=True)

    # ключевой момент: id как строка, чтобы Excel не делал 7E+16 (атрибут 'столбец' редактируется, а не добавляется)
    # берём существующий столбец id -> приводим тип значений в этом столбце к str -> перезаписываем этот же столбец
    df_all["id"] = df_all["id"].astype(str)
    # Сортировка данных во фрейме по запросу и по адресу (а не по времени и источнику добавления)
    df_all = df_all.sort_values(by=["query", "address"])

    df_all.to_excel(filepath, index=False)
    wb = load_workbook(filepath)  # Рабочая книга openpyxl - начало пути к переоформлению таблицы в Excel
    ws = wb.active  # Рабочий лист в таблице openpyxl
    end_row = ws.max_row  # Посчитать диапазон:  - здесь для а) строк
    end_col = ws.max_column  # - здесь для б) столбцов
    # Диапазон таблицы в openpyxl с учётом диапазона строк и столбцов, посчитанных выше
    ref = f"A1:{chr(64 + end_col)}{end_row}"
    # Создание таблицы (задаётся отображаемое имя)
    table = Table(displayName="LeadsTable", ref=ref)
    # Создание стиля таблицы
    style = TableStyleInfo(
        name="TableStyleMedium9",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=True,
        showColumnStripes=False,
    )

    table.tableStyleInfo = style
    ws.add_table(table)
    # Сохранение оформления таблицы в файл по ранее определённому пути
    wb.save(filepath)

    print(f"Всего лидов в мастер-файле: {len(df_all)}")
