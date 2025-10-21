# 🧠 Dynamic Painless Script Generator

This project automates the generation of **Elasticsearch Painless scripts** by reading **rule definitions from Excel**.  
It dynamically constructs complex aggregation logic (`scripted_metric`) for **Numerator** and **Denominator** calculations — saving hours of manual effort and ensuring consistency across multiple rule sets.

---

## 🚀 Features

- ✅ Reads business rules from an Excel file (`rules.xlsx`)
- ⚙️ Automatically parses rules (e.g., *Filter*, *Exclude*, *Unfilter*, *Contain*, etc.)
- 🔄 Dynamically generates a valid **Elasticsearch aggregation JSON**
- 📄 Outputs a clean, production-ready `generated_painless.json` file
- 🧩 Easily adaptable to any metric or rule definition

---

## 📂 Project Structure

