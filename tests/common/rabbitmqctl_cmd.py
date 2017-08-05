import os


def create_user(user_name, password):
    os.system('sudo rabbitmqctl add_user %s %s' % (user_name, password))
    os.system('sudo rabbitmqctl set_user_tags %s administrator' % user_name)
    os.system('sudo rabbitmqctl set_permissions -p "/" %s ".*" ".*" ".*"' % user_name)
    os.system('sudo rabbitmqctl list_users')


def delete_user(user_name):
    os.system('sudo rabbitmqctl delete_user %s' % user_name)
