# Generated by Django 3.2.8 on 2022-12-13 12:20

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("datatransform", "0006_auto_20220915_1648"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="pipeline",
            name="dataset_id",
        ),
        migrations.RemoveField(
            model_name="pipeline",
            name="resource_id",
        ),
        migrations.AddField(
            model_name="pipeline",
            name="resultant_res_id",
            field=models.CharField(default=1, max_length=50),
            preserve_default=False,
        ),
    ]
