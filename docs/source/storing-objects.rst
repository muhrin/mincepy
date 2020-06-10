Storing Objects
+++++++++++++++

Unlike many ORMs, mincePy does not require that the object to be stored are subclasses of a mincePy type.
This is a deliberate choice and means that it is possible to store objects that you cannot, or do not want to make inherit from anything in mincePy.  This is all possible thanks to the :class:`mincepy.TypeHelper` which is used to tell mincePy all the things it needs to know to be able to work with them.  That said, if you've got a type want to use specifically with mincePy you can subclass from :class:`mincepy.SavableObject` (or one of its subclasses).


Concepts
========

Migrations
==========

Decided you want to change the way you store your objects?  Have a database with thousands of precious objects stored in the, soon to be, old format?  Not ideal...

Don't worry, we've got your back.  Migrations are a way to tell mincePy how to go from the old version to the new.  Imagine we have a ``Car`` object that looks like this:

.. code-block:: python

    class Car(mincepy.SimpleSavable):
        TYPE_ID = uuid.UUID('297808e4-9bc7-4f0a-9f8d-850a5f558663')
        ATTRS = ('colour', 'make')

        def __init__(self, colour: str, make: str):
            super(Car, self).__init__()
            self.colour = colour
            self.make = make

        def save_instance_state(self, saver: mincepy.Saver):
            super(Car, self).save_instance_state(saver)
            # Here, I decide to store as an array
            return [self.colour, self.make]

        def load_instance_state(self, saved_state, loader: mincepy.Loader):
            self.colour = saved_state[0]
            self.make = saved_state[1]

Great, so now let's save some cars!


.. code-block:: python

    Car('red', 'zonda').save()
    Car('black', 'bugatti').save()
    Car('white', 'ferrari').save()

Ok, and now we decide that instead of storing the details as a list, we want to use a dictionary instead.  No problem, let's just put in place a migration:

.. code-block:: python

    class Car(mincepy.SimpleSavable):
        TYPE_ID = uuid.UUID('297808e4-9bc7-4f0a-9f8d-850a5f558663')
        ATTRS = ('colour', 'make')

        class Migration1(mincepy.ObjectMigration):
            VERSION = 1

            @classmethod
            def upgrade(cls, saved_state, migrator):
                return dict(colour=saved_state[0], make=saved_state[1])

        # Set the migration
        LATEST_MIGRATION = Migration1

        def save_instance_state(self, saver):
            # I've changed my mind, I'd like to store it as a dict
            return dict(colour=self.colour, make=self.make)

        def load_instance_state(self, saved_state, loader):
            self.colour = saved_state['colour']
            self.make = saved_state['make']


Here, we've changed :meth:`~mincepy.Savable.save_instance_state` and :meth:`~mincepy.Savable.load_instance_state` as expected.
Then, we create a subclass of :class:`mincepy.ObjectMigration` which implements the :func:`~mincepy.ObjectMigration.upgrade` class method.
This method gets an old saved state and is tasked with returning a state in the new format.
So, we take the old array and return it as a dictionary that will be understood by our new :meth:`~mincepy.Savable.load_instance_state`.

Next, we set the ``VERSION`` number for our migration.  This should be an integer that is higher than the last migration.  As we have no migration, ``1`` will do.

Finally, we tell the ``Car`` object what the latest migration is by setting the ``LATEST_MIGRATION`` class attribute to the migration class.

All this will allow ``mincepy`` to load your objects by converting them to the current format as needed.  The database, however, will not be touched unless you save the object again after making changes, in which case it will saved with the new form.

Performing Migrations
---------------------

To update the state of all objects in your database you can use the command line command:

.. code-block:: python

    mince migrate mongodb://localhost/my-database

Where the databases URI is supplied as the argument.  This will inform you how many records are to be migrated and allow you to perform the migration.

Adding Migrations
-----------------

If you decide you want to change the format of ``Car`` again, say by adding a registration field, it can be done like this:

.. code-block:: python

    class Car(mincepy.SimpleSavable):
        TYPE_ID = uuid.UUID('297808e4-9bc7-4f0a-9f8d-850a5f558663')
        ATTRS = ('colour', 'make')

        class Migration1(mincepy.ObjectMigration):
            VERSION = 1

            @classmethod
            def upgrade(cls, saved_state, migrator):
                return dict(colour=saved_state[0], make=saved_state[1])

        class Migration2(mincepy.ObjectMigration):
            VERSION = 2
            PREVIOUS = Migration1

            @classmethod
            def upgrade(cls, saved_state, migrator):
                # Augment the saved state
                saved_state['reg'] = 'unknown'
                return saved_state

        # Set the migration
        LATEST_MIGRATION = Migration2

        def __init__(self, colour: str, make: str, reg=None):
            super(Car, self).__init__()
            self.colour = colour
            self.make = make
            self.reg = reg

        def save_instance_state(self, saver: mincepy.Saver):
            # I've changed my mind, I'd like to store it as a dict
            return dict(colour=self.colour, make=self.make, reg=self.reg)

        def load_instance_state(self, saved_state, loader):
            self.colour = saved_state['colour']
            self.make = saved_state['make']
            self.reg = saved_state['reg']

This migration was added using the following steps:

    1. Created ``Migration2`` with an `upgrade` method that adds the missing data,
    2. Set ``Migration2.VERSION`` to a version number higher than the previous
    3. Set the ``PREVIOUS`` class attribute to the previous migration.  This way, mincePy can upgrade all the way from the original version to the latest.
    4. Set ``Car``'s ``LATEST_MIGRATION`` to point to th enew migration

Again, this is enough to load and save old versions, however to make the changes to the database records use the  `migrate` tool described above.