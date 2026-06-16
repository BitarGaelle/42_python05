# Code Nexus — Polymorphic Data Streams

## 📌 Summary
Code Nexus is a Python project that builds a **polymorphic data processing pipeline** using abstract classes, inheritance, and plugin-based architecture.

It demonstrates how different data types can be processed, stored, and exported in a unified system.

---

## 🧠 Concept

The system simulates a data flow pipeline:
Input Stream → DataStream → Processors → Output Pipeline → Export Plugins


Each layer is independent and communicates through **polymorphism and duck typing**.

---

## 📁 Structure

- `ex0/` — Base data processors (ABC, validation, ingestion, output)
- `ex1/` — DataStream (polymorphic routing of mixed data)
- `ex2/` — Data pipeline with export plugins (CSV / JSON)

---

## 🔹 Key Features

### Exercise 0
- Abstract `DataProcessor`
- `NumericProcessor`, `TextProcessor`, `LogProcessor`
- Validate, ingest, and output data with rank tracking

### Exercise 1
- `DataStream` class
- Automatic routing using `validate()`
- Processor statistics tracking

### Exercise 2
- `ExportPlugin` interface (Protocol)
- CSV and JSON exporters
- `output_pipeline()` for batch export

---

## 🧠 What is a Protocol?

A **Protocol** is a type hint that defines a required behavior (methods and signatures) without enforcing inheritance.

👉 In simple terms:
- It defines *what methods a class must have*
- Not *what class it must inherit from*

This enables **duck typing**, meaning:
> “If it behaves like a plugin, it is a plugin.”

---

## 🧠 Key Concepts Learned

- Abstract Base Classes (ABC)
- Polymorphism
- Method overriding
- Duck typing
- Plugin architecture
- Data pipelines

---

## 🚀 Goal

Build a flexible system where:
- Data is processed without knowing its type
- Output formats can be swapped easily
- New processors/plugins can be added without modifying core logic

---

## ⚙️ Requirements

- Python 3.10+
- `abc`, `typing`
- Flake8 compliant
- MyPy type annotations