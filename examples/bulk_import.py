import gevent
from gevent import monkey; monkey.patch_all()
import math
import time
from graph import ttypes
from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient, ExecutionException, AuthException

class Nebula:
	def __init__(self, ip='127.0.0.1', port=3699, user='user', password='password', 
						space_name='test', socket_num=5, is_async=True, network_timeout=5000,
						partition_num=100, replica_factor=1):
		
		if socket_num < 1:
			socket_num = 1
		self.socket_num = socket_num
		self.pool = ConnectionPool(ip, port, socket_num, is_async, network_timeout)

		self.clients = []
		for n in range(socket_num):
			client_temp = GraphClient(self.pool)
			auth_resp = client_temp.authenticate(user, password)
			if auth_resp.error_code:
				raise AuthException("Auth failed")
			self.clients.append(client_temp)	
			query_resp = client_temp.execute_query('SHOW SPACES')
			if not self.has_space(query_resp.rows, space_name):
				self.do_execute('CREATE SPACE %s(partition_num=%d, replica_factor=%d)'% (space_name,
					partition_num, replica_factor), n)
			self.do_execute('USE %s' % space_name, n)
			
	def has_space(self, rows, space_name):
		for row in rows:
			if len(row.columns) != 1:
				raise ExecutionException('The row of SHOW SPACES has wrong size of columns')
			if row.columns[0].get_str().decode('utf-8') == space_name:
				return True
		return False

	def do_execute(self, cmd, num=0):
		index = num % self.socket_num
		client = self.clients[index]
		resp = client.execute(cmd)
		if resp.error_code != 0:
			print('Execute error msg: %s' % (resp.error_msg))
	
	def do_execute_query(self, cmd, num=0):
		index = num % self.socket_num
		client = self.clients[index]
		query_resp = client.execute_query(cmd)
		if query_resp.error_code:
			print('Execute failed: %s' % query_resp.error_msg)
		return query_resp
		
	def do_bulk_execute(self, cmds):
		length = len(cmds)
		num = self.socket_num
		tasks = []
		for index, cmd in enumerate(cmds):
			tasks.append(gevent.spawn(self.do_execute, cmd, index))
			if ((index+1)%num==0) or (index == length-1):
				gevent.joinall(tasks)
		return None
		
	
if __name__=='__main__':
	nb = Nebula(ip='127.0.0.1', port=3699, user='user', password='password', space_name='test', socket_num=50)
	
	# nb.do_execute('DROP SPACE test')
	
	schema = 'CREATE TAG node(name string); CREATE EDGE relation(name string)'
	nb.do_execute(schema) # 需要挺长时间的
	
	
	node1 = 'INSERT VERTEX node(name) VALUES hash("姚明"):("姚明"), hash("姚明"):("姚明");'
	node2 = 'INSERT VERTEX node(name) VALUES hash("叶丽"):("叶丽"), hash("叶丽"):("叶丽");'
	relation = 'INSERT EDGE relation(name) VALUES hash("姚明") -> hash("叶丽"):("老婆"), hash("姚明") -> hash("叶丽"):("老婆");'
	

	cmds = [node1, node2, relation]
	

	nb.do_bulk_execute(cmds)
		



