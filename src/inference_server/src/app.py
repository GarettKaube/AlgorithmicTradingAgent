from pydantic import BaseModel
from contextlib import asynccontextmanager
import logging

from datetime import datetime, timedelta, timezone

from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.security import OAuth2PasswordBearer

import jwt
from jwt.exceptions import InvalidTokenError
from passlib.context import CryptContext

import mlflow
import pandas as pd
import os
from mlflow import MlflowClient

import json


logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)

config = "./config"
with open("./config/mlflow_server_uri.json", 'r') as f:
    tracking_uri = json.load(f)['uri']

models = {}

# Get password
pswd = os.environ["my_secret"]

# Get secret key
SECRET_KEY = os.environ["sec_key"]
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 30


class MLflowInstance:
    def __init__(self, uri:str) -> None:
        mlflow.set_tracking_uri(uri)
        self.client = mlflow.MlflowClient(tracking_uri=uri)
        self.model_name = "LSTMProduction@latest"


    def check_server_health(self):
        try:
            experiments = self.client.search_experiments()
            return "Mlflow ok"
        except:
            return "MLflow connection error"
        

    def get_prod_model(self, model_name):
        """ Gets the model for forecasting returns
        """
        model_name = model_name.strip()
        os.makedirs(f"./models/{model_name}", exist_ok=True)
        model = mlflow.sklearn.load_model(
            model_uri = f"models:/{model_name}/latest", 
            dst_path=f"models/{model_name}",
        )
        
        pyfunc_model = mlflow.pyfunc.load_model(f"models:/{model_name}/latest")
        return model
    

@asynccontextmanager
async def setup(app: FastAPI):
    global mlflow_inst
    mlflow_inst = MLflowInstance(tracking_uri)
    logging.info(f"Connected to MLflow with URI: {tracking_uri}")
    yield
    models.clear()


app = FastAPI(lifespan=setup)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Implement security 
fake_users_db = {
    "bobjames": {
        "username": "bobjames",
        "full_name": "Bob James",
        "email": "bobjames@example.com",
        "hashed_password": pswd,
        "disabled": False,
    }
}


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: str | None = None


class User(BaseModel):
    username: str
    email: str | None = None
    full_name: str | None = None
    disabled: bool | None = None


class UserInDB(User):
    hashed_password: str


def verify_password(plain_password, hashed_password):
    print(plain_password, hashed_password)
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)


def authenticate_user(fake_db, username: str, password: str):
    user = get_user(fake_db, username)
    if not user:
        print("User not found")
        return False
    if not verify_password(password, user.hashed_password):
        print("incorrect password")
        return False
    return user


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except InvalidTokenError:
        raise credentials_exception
    user = get_user(fake_users_db, username=token_data.username)
    if user is None:
        raise credentials_exception
    return user

async def get_current_active_user(
    current_user: Annotated[User, Depends(get_current_user)],
):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


@app.post("/token")
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return Token(access_token=access_token, token_type="bearer")




class Request(BaseModel):
    model_name: str
    date: list[str]
    features: list[str]
    X: list


class getRequest(BaseModel):
    model_name: str


@app.get("/health/", status_code=200)
async def health_check():
    logging.info("Checking health")
    return {
        "MLFlow health": mlflow_inst.check_server_health()
    }


async def get_model(model_name):
    if mlflow_inst.model_name not in models:
        models[mlflow_inst.model_name] = mlflow_inst.get_prod_model(model_name)

    return models[mlflow_inst.model_name]


def fix_mlflow_code_path(model_name):
    from os.path import join
    from os import listdir, rmdir
    from shutil import move

    root = f'models/{model_name}/code'
    for filename in listdir(join(root, 'modeling')):
        move(join(root, 'modeling', filename), join(root, filename))
    rmdir(join(root, 'modeling'))



def inference(request, model):
    # Store model predictions
    result = {}
    # Convert the request to a dataframe
    X = pd.DataFrame(request.X, index=request.date, columns=request.features)
    
    # Use the model to generate predictions and probabilities
    predictions = []
    probabilities = []
    
    pred = model.predict(X)
    predictions.append(pred.item())
    probabilities = model.predict_proba(X)[0][int(pred)]
    forecasts = pd.DataFrame({"direction": predictions, "confidence": probabilities}, index=request.date)
    
    result["request"] = request.model_dump()
    result['Forecast'] = forecasts.to_dict()

    return result


@app.post("/forecast/", status_code=200)
async def return_forecasts(request:Request, token: Annotated[str, Depends(oauth2_scheme)]):
    # Get the model
    model_name = request.model_name
    model = await get_model(model_name)

    result = inference(request, model=model)
    return result


@app.get("/model_life/", status_code=200)
async def model_life(request:getRequest):
    result = {}
    model_name = request.model_name

    client = MlflowClient()

    # Get the latest version of the model
    latest_version = client.get_latest_versions(model_name, stages=["None", "Staging", "Production"])[0]

    # Get the run details for the model
    run_id = latest_version.run_id
    run = client.get_run(run_id)

    # Extract and print the creation date (timestamp is in milliseconds)
    creation_timestamp = run.info.end_time
    creation_date = datetime.datetime.fromtimestamp(creation_timestamp / 1000.0)

    result["model_creation_date"]= creation_date
    result["created_days_ago"] = (datetime.datetime.now() - creation_date).days
    return result
