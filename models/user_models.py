from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

class UserProfileResponse(BaseModel):
    id: str
    username: str
    email: str
    role: str

class CreateUserRequest(BaseModel):
    username: str
    email: str
    password: str
    role: str

class UpdateUserRequest(BaseModel):
    username: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    password: Optional[str] = None

class UserResponse(BaseModel):
    id: str
    username: str
    email: str
    role: str

class UserResponseWithMessage(BaseModel):
    success: bool
    message: str
    user: Optional[UserResponse] = None

class DeleteUserResponse(BaseModel):
    success: bool
    message: str 