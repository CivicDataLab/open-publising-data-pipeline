from os import name, stat_result
from django.db import models
import datetime

# Create your models here.


class Pipeline(models.Model):
    pipeline_id    = models.AutoField(primary_key=True)
    created_at     = models.DateTimeField(default=datetime.datetime.now) 
    status         = models.CharField(max_length=50) 


class Task(models.Model):
    task_id        = models.AutoField(primary_key=True)
    task_name      = models.CharField(max_length=50)
    context        = models.CharField(max_length=500)
    status         = models.CharField(max_length=50)
    order_no       = models.IntegerField()
    Pipeline_id    = models.ForeignKey(Pipeline, on_delete=models.CASCADE)



