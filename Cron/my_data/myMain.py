import OrderToBQ
# import statusCancelUpdate
import notSetOrders
import SIDGa
import AFOrdersPaid

OrderToBQ.main()
# statusCancelUpdate.main()
notSetOrders.main()
SIDGa.main()
AFOrdersPaid.main()
with open(r'c:\python\ukt\Cron\log\OrdersFullToBQ.txt', 'a') as f:
    f.write(75*'-'+'\n')