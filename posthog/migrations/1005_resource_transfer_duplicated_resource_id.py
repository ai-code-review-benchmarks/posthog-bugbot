# Generated manually

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("posthog", "1004_resource_transfer"),
    ]

    operations = [
        migrations.AddField(
            model_name="resourcetransfer",
            name="duplicated_resource_id",
            field=models.CharField(default="", max_length=100),
            preserve_default=False,
        ),
    ]
