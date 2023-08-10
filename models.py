from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

class Employee(BaseModel):
    __tablename__ = "employees"
    id: int
    name: str
    department: str

DATABASE_URL = "sqlite:///./employee.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def create_employee_table():
    Base.metadata.create_all(bind=engine)
