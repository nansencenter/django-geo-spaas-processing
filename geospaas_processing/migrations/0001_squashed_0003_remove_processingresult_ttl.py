# Generated by Django 3.2 on 2022-10-04 07:20

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    replaces = [('geospaas_processing', '0001_initial'), ('geospaas_processing', '0002_auto_20221003_1430'), ('geospaas_processing', '0003_remove_processingresult_ttl')]

    initial = True

    dependencies = [
        ('catalog', '0011_auto_20210525_1252'),
    ]

    operations = [
        migrations.CreateModel(
            name='ProcessingResult',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('path', models.FilePathField(max_length=500, unique=True)),
                ('type', models.CharField(choices=[('syntool', 'Syntool metadata file'), ('idf', 'IDF file')], max_length=20)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='catalog.dataset')),
            ],
        ),
    ]
