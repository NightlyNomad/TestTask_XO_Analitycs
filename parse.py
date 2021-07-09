from pprint import pprint
import pandas as pd
import httplib2
import googleapiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials


# Расшарить таблицу для сервисного аккаунта
# task-672@testtask-319214.iam.gserviceaccount.com

# Global
CREDENTIALS_FILE = 'testtask-319214-76ec5d1bb753.json'
# Задание
task_spreadsheet_id = '1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ'
# Выгрузка
load_spreadsheet_id = '10tajSmXAU8ogo8TfAnVgGWLHryqKT818Upi25-_qcj8'


class Sheet(object):

	def __init__(self, spreadsheet_id):
		# Авторизуемся и получаем service — экземпляр доступа к API
		credentials = ServiceAccountCredentials.from_json_keyfile_name(
			CREDENTIALS_FILE,
			['https://www.googleapis.com/auth/spreadsheets',
			 'https://www.googleapis.com/auth/drive'])
		httpAuth = credentials.authorize(httplib2.Http())
		self.service = googleapiclient.discovery.build('sheets', 'v4', http=httpAuth)
		self.spreadsheet_id = spreadsheet_id

	# Получить значения ячеек в диапазоне
	def get_values(self, range_name, majorDimension = 'ROWS'):   # ROWS/COLUMNS
		# Реализуем запрос
		value = self.service.spreadsheets().values().get(
			spreadsheetId=self.spreadsheet_id,
			range=range_name,
			majorDimension=majorDimension
		).execute()
		# Форматируем ответ
		return value['values']

	# Записать пул значений в таблицу
	def write_values(self, range_name, value, majorDimension = 'ROWS'): # ROWS/COLUMNS
		self.service.spreadsheets().values().batchUpdate(
			spreadsheetId=self.spreadsheet_id,
			body={
				"valueInputOption": "USER_ENTERED",
				"data":
					{"range": range_name,
					 "majorDimension": majorDimension,
					 "values": value},
			}
		).execute()


# Инициализируем таблицу для сбора данные
task_sheet = Sheet(task_spreadsheet_id)
# Инициализируем таблицу для загрузки данных
load_sheet = Sheet(load_spreadsheet_id)

# Парсим данные

# managers
managers_title = task_sheet.get_values('managers!A1:C1')
managers_value = task_sheet.get_values('managers!A2:C')
managers_frame = pd.DataFrame(managers_value, columns=managers_title[0]).reset_index()

# transactions
transactions_title = task_sheet.get_values('transactions!A1:D1')
transactions_value = task_sheet.get_values('transactions!A2:D')
transactions_frame = pd.DataFrame(transactions_value, columns=transactions_title[0]).reset_index()

# clients
clients_title = task_sheet.get_values('clients!A1:C1')
clients_value = task_sheet.get_values('clients!A2:C')
clients_frame = pd.DataFrame(clients_value, columns=clients_title[0]).reset_index()

# leads
leads_title = task_sheet.get_values('leads!A1:F1')
leads_value = task_sheet.get_values('leads!A2:F')
leads_frame = pd.DataFrame(leads_value, columns=leads_title[0]).reset_index()

# обрабатываем данные

# Подготовка
leads_frame['d_utm_source'] = leads_frame['d_utm_source'].replace('insta','instagram')
leads_frame['d_utm_source'] = leads_frame['d_utm_source'].replace('vk','vkontakte')
leads_frame['d_utm_source'] = leads_frame['d_utm_source'].replace('ycard#!/tproduct/225696739-1498486363994','ycard')
leads_frame['d_utm_source'] = leads_frame['d_utm_source'].replace('ig','instagram')
leads_frame['d_utm_source'] = leads_frame['d_utm_source'].replace('','undefiend')


# Количество заявок
# Находим количество заявок на которые назначен менеджер, следовательно обработанные.
amount_of_leads_frame = leads_frame.merge(managers_frame, left_on='l_manager_id', right_on='manager_id')
# Выполняем группировку по целевым параметрам
amount_of_leads = amount_of_leads_frame.groupby(['d_utm_source','d_club','d_manager']).agg(Количество_заявок=('l_client_id', 'size'))

# Количество мусорных заявок (на основании заявки не создан клиент)
# Находим пересечение заявок и пользователей
amount_of_notrubbish_leads_frame = amount_of_leads_frame.merge(clients_frame, left_on='l_client_id', right_on='client_id')
# Группируем количество заявок, которым соответствуют пользователи
amount_of_notrubbish_leads = amount_of_notrubbish_leads_frame.groupby(['d_utm_source','d_club','d_manager']).agg(Количество_заявок=('l_client_id', 'size'))
# Находим величину "мусорных заявок"
amount_of_rubbish_leads = amount_of_leads - amount_of_notrubbish_leads
amount_of_rubbish_leads = amount_of_rubbish_leads.rename(columns={'Количество_заявок':'Количество_мусорных_заявок'})

# Количество новых заявок (не было заявок и покупок от этого клиента раньше)
# Удаляем заявки, от одного и того же клиента, оставив самые старые
oldest_leads_of_users = amount_of_notrubbish_leads_frame.groupby(['d_utm_source','d_club','d_manager','l_client_id']).agg(Дата_заявки=('created_at_x', 'min')).reset_index()
# Присоединяем покупки
trans_first_leads_frame = oldest_leads_of_users.merge(transactions_frame, left_on='l_client_id', right_on='l_client_id', how = 'left')
# Удаляем покуки позже даты запроса
trans_first_leads_frame = trans_first_leads_frame.groupby(['d_utm_source','d_club','d_manager','l_client_id','Дата_заявки']).agg(Дата_покупки=('created_at', 'min')).reset_index()
trans_first_leads_frame = trans_first_leads_frame[trans_first_leads_frame['Дата_заявки'] < trans_first_leads_frame['Дата_покупки']]
amount_of_new_users = trans_first_leads_frame.groupby(['d_utm_source','d_club','d_manager']).agg(Количество_новых_заявок=('l_client_id', 'size'))

# количество покупателей (кто купил в течение недели после заявки)
amout_of_buyer_frame = amount_of_notrubbish_leads_frame.merge(transactions_frame, left_on='l_client_id', right_on='l_client_id', how = 'left')
amout_of_buyer_frame['created_at_x'] = pd.to_datetime(amout_of_buyer_frame['created_at_x'])
amout_of_buyer_frame['created_at'] = pd.to_datetime(amout_of_buyer_frame['created_at'])
week = pd.Timedelta(1,unit='w')
amout_of_buyer_frame = amout_of_buyer_frame[(amout_of_buyer_frame['created_at_x'] < amout_of_buyer_frame['created_at']) & (amout_of_buyer_frame['created_at'] < amout_of_buyer_frame['created_at_x'] + week)]
amout_of_buyer_frame = amout_of_buyer_frame.groupby(['d_utm_source','d_club','d_manager','l_client_id']).agg(Количество=('created_at', 'size')).reset_index()
amout_of_buyer = amout_of_buyer_frame.groupby(['d_utm_source','d_club','d_manager']).agg(Количество_покупателей=('l_client_id', 'size'))

# количество новых покупателей (кто купил в течение недели после заявки, и не покупал раньше)
trans_first_leads_frame['Дата_покупки'] = pd.to_datetime(trans_first_leads_frame['Дата_покупки'])
trans_first_leads_frame['Дата_заявки'] = pd.to_datetime(trans_first_leads_frame['Дата_заявки'])
trans_first_leads_frame = trans_first_leads_frame[trans_first_leads_frame['Дата_покупки'] < trans_first_leads_frame['Дата_заявки'] + week]
amount_new_buyer = trans_first_leads_frame.groupby(['d_utm_source','d_club','d_manager']).agg(Количество_новых_покупателей=('l_client_id', 'size'))

# доход от покупок новых покупателей
value_of_new_user_frame = trans_first_leads_frame.merge(transactions_frame, on='l_client_id')
value_of_new_user_frame['m_real_amount'] = value_of_new_user_frame['m_real_amount'].astype(int)
value_of_new_user_frame = value_of_new_user_frame.groupby(['d_utm_source','d_club','d_manager','l_client_id']).agg(Доход=('m_real_amount', 'sum')).reset_index()
value_of_new_user = value_of_new_user_frame.groupby(['d_utm_source','d_club','d_manager']).agg(Доход_от_новых_покупателей=('Доход', 'sum'))

# Формируем итоговую таблицу
# конкантинируем фреймы
itog = pd.concat([amount_of_leads,amount_of_rubbish_leads, amount_of_new_users, amout_of_buyer, amount_new_buyer, value_of_new_user], axis=1)
# переименовываем пустые значения
itog = itog.fillna(0)
itog = itog.reset_index()

# # Загружаем данные

load_sheet.write_values('Лист1!A1:I', [list(itog.columns)])
load_sheet.write_values('Лист1!A2:I', itog.values.tolist())