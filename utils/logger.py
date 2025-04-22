# Databricks notebook source
# utils/logger.py

from datetime import datetime

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level.upper()}] {msg}")

def log_info(msg: str):
    log(msg, "INFO")

def log_warning(msg: str):
    log(msg, "WARNING")

def log_error(msg: str):
    log(msg, "ERROR")
