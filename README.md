ev-data-engineering-pipeline/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── data/
│   ├── bronze_raw/
│   ├── silver_clean/
│   └── samples/
│
├── notebooks/
│   ├── exploration.ipynb
│   └── cleaning_tests.ipynb
│
├── src/
│   ├── scraping/
│   │   └── scrape_ev_specs.py
│   ├── processing/
│   │   └── clean_transform.py
│   ├── database/
│   │   ├── create_tables.sql
│   │   └── load_to_mysql.py
│   └── utils/
│       └── helpers.py
│
├── logs/
│   └── pipeline.log
│
└── docs/
    ├── architecture_diagram.png
    ├── entity_relationship_diagram.png
    └── proposal.pdf
