from django.urls import path
from .views import upload_files, display_data, upload_success, check_task_status

urlpatterns = [
    path('upload/', upload_files, name='upload'),
    path('display/', display_data, name='display_data'),
    path('upload/success/', upload_success, name='success'),
    path('task_status/', check_task_status, name='check_task_status'),

]