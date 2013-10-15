/*
Creates stored procedures used to track availability and usage of our development stacks.
*/

use stacks;
	
drop procedure if exists list_stacks;
drop procedure if exists list_users;
drop procedure if exists reserve_stack;
drop procedure if exists reserve_stack2;
drop procedure if exists release_stack;
drop procedure if exists subscribe_checkins;
drop procedure if exists subscribe_checkouts;

DELIMITER // 

/* Lists the stacks and their availability. */
create procedure list_stacks()
begin
	select 	stack as Stack, 
		userModule as 'User Module',
		status as Status, 
		ifnull(user, '') User, 
		ifnull(checkedout, '') 'Checked Out', 
		ifnull(timediff(now(), checkedout), '') Time,
	        ifnull(notes, '') Notes,
		version Version
	from stacks 
	order by stack;
end // 

/* Lists the users. */
create procedure list_users()
begin
	select * from users order by 1;
end // 

create procedure reserve_stack(in inStack varchar(20), in inUser varchar(20))
begin
	call reserve_stack2(inStack, inUser, 'commandLine');	
end //

/* Reserves the given stack on behalf of the given user. */
create procedure reserve_stack2(in inStack varchar(20), in inUser varchar(20), in inSource varchar(20))
begin
	declare cnt int default 0;
	declare err varchar(100) default '';

	/* Validate the stack exists. */
	select count(*) into cnt from stacks where upper(stack) = upper(inStack);
	if cnt <> 1 then
		set err = concat(inStack, ' is not a valid stack.');
	end if;

	/* Validate that the stack is not already checked out unless the user is nightly in which case he gets it regardless of it */
    /* being checked out. */
	if lower(inUser) <> 'nightly' and err = '' then
		select count(*) into cnt from stacks where status='Available' and upper(stack) = upper(inStack);
        if cnt <> 1 then
            set err = concat(inStack, ' is not available.');
		end if;
    end if;

	/* Do the check out. */
	if err = '' then

		update stacks 
		set status='Checked Out', user=inUser, checkedout=now() 
		where upper(stack)=upper(inStack) and (status='Available' or lower(inUser)='nightly');

        insert into stacksLog values (inStack, inUser, now(), null, inSource);

	end if;

	if err <> '' then
		select err Status;	
	else
		select concat(inStack, ' successfully checked out by ', inUser, '.') Status;
	end if;
end //

/* Releases the given stack.  Requires the passed user to be the one holding the reservation. */
create procedure release_stack(in inStack varchar(20), in inUser varchar(20))
begin
	declare cnt int default 0;
	declare err varchar(100) default '';

	/* Validate the stack exists. */
	select count(*) into cnt from stacks where upper(stack) = upper(inStack);
	if cnt <> 1 then
		set err = concat(inStack, ' is not a valid stack.');
	end if;

	/* Validate that the stack is checked out by the user. */
	if err = '' then
		select count(*) into cnt 
		from stacks 
		where status='Checked Out' and upper(stack) = upper(inStack) and upper(user) = upper(inUser);

	    if cnt <> 1 then
            set err = concat(inStack, ' is not checked out by ', inUser, '.');
    	end if;
    end if;

	/* Update the row to release_stack the stack. */
	if err = '' then
		update stacks 
		set status='Available', user=null, checkedout=null 
		where upper(stack)=upper(inStack);

	        update stacksLog
        	set checkedIn=now()
	        where upper(stack) = upper(inStack) and checkedIn is null and upper(user) = upper(inUser);
	end if;

	if err <> '' then
		select err Status;	
	else
		select concat(inStack, ' successfully released.') Status;
	end if;
end //

create procedure subscribe_checkins(in inUser varchar(20), in inSubscribe bool)
begin
    declare cnt int default 0;
	declare err varchar(100) default '';
    
    /* Validate user. */
    select count(*) into cnt from users where upper(user)=upper(inUser);
    if cnt <> 1 then
        set err = concat(inUser, ' is not a valid user.');
    end if;

    if err = '' then
        if inSubscribe then
            update users
            set mailOnCheckins = true
            where upper(user) = upper(inUser);
        else
            update users
            set mailOnCheckins = false
            where upper(user) = upper(inUser);
        end if;
    end if;
	
    if err <> '' then
		select err Status;	
	else
		select concat(inUser, ' checkin mail setting successfully set.') Status;
	end if;
end //

create procedure subscribe_checkouts(in inUser varchar(20), in inSubscribe bool)
begin
    declare cnt int default 0;
	declare err varchar(100) default '';
    
    /* Validate user. */
    select count(*) into cnt from users where upper(user)=upper(inUser);
    if cnt < 1 then
        set err = concat(inUser, ' is not a valid user.');
    end if;

    if err = '' then
        if inSubscribe then
            update users
            set mailOnCheckouts = true
            where upper(user) = upper(inUser);
        else
            update users
            set mailOnCheckouts= false
            where upper(user) = upper(inUser);
        end if;
    end if;
    
    if err <> '' then
		select err Status;	
	else
		select concat(inUser, ' checkout mail setting successfully set.') Status;
	end if;
end //

DELIMITER ;

