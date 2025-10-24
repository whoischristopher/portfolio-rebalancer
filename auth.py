import os
from flask import Blueprint, redirect, url_for, session, flash, request
from flask_login import login_user, logout_user, current_user
from authlib.integrations.flask_client import OAuth
from models import db, User
from datetime import datetime

auth_bp = Blueprint('auth', __name__)
oauth = OAuth()

def init_oauth(app):
    '''Initialize OAuth with the Flask app'''
    oauth.init_app(app)
    oauth.register(
        name='google',
        client_id=os.getenv('GOOGLE_CLIENT_ID'),
        client_secret=os.getenv('GOOGLE_CLIENT_SECRET'),
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={
            'scope': 'openid email profile'
        }
    )

@auth_bp.route('/login')
def login():
    '''Redirect to Google OAuth login page'''
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    
    # Generate redirect URI with HTTPS
    redirect_uri = url_for('auth.callback', _external=True, _scheme='https')
    return oauth.google.authorize_redirect(redirect_uri)

@auth_bp.route('/callback')
def callback():
    '''Handle OAuth callback from Google'''
    try:
        # Get access token
        token = oauth.google.authorize_access_token()
        user_info = token.get('userinfo')
        
        if not user_info:
            flash('Authentication failed. Please try again.', 'error')
            return redirect(url_for('index'))
        
        google_id = user_info.get('sub')
        email = user_info.get('email')
        
        # Check if user exists
        user = User.query.filter_by(google_id=google_id).first()
        
        if not user:
            # Create new user
            user = User(
                google_id=google_id,
                email=email,
                name=user_info.get('name', ''),
                picture=user_info.get('picture', ''),
                created_at=datetime.utcnow(),
                last_login=datetime.utcnow()
            )
            db.session.add(user)
            db.session.commit()
            flash(f'Welcome {user.name}! Your account has been created.', 'success')
        else:
            # Update last login
            user.last_login = datetime.utcnow()
            db.session.commit()
            flash(f'Welcome back, {user.name}!', 'success')
        
        # Log user in
        login_user(user, remember=True)
        
        # Redirect to next page or dashboard
        next_page = request.args.get('next')
        return redirect(next_page) if next_page else redirect(url_for('dashboard'))
        
    except Exception as e:
        print(f"Authentication error: {e}")
        flash('Authentication failed. Please try again.', 'error')
        return redirect(url_for('index'))


@auth_bp.route('/logout')
def logout():
    '''Log out current user'''
    logout_user()
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('index'))

