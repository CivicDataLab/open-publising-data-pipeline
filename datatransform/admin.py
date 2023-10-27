from django.contrib import admin

# Register your models here.
from .models import Pipeline, Task

admin.site.register(Task)
admin.site.register(Pipeline)
