import React from 'react';
import {Routes, Route} from 'react-router-dom';
import Calendar from './pages/Calendar';
import WritingDiaryPage from './pages/WritingDiaryPage';
import DrawCompletedPage from './pages/DrawCompletedPage';
import SelectDrawerPage from './pages/SelectDrawerPage';
import DrawerListPage from './pages/DrawerListPage';
import AlbumThumbnailPage from './pages/AlbumThumbnailPage';
import AlbumListPage from './pages/AlbumListPage';
import SelectEmotionPage from './pages/SelectEmotionPage';
import GlobalStyle from './GlobalStyle';

function App() {
  return (
    <>
      <GlobalStyle />
      <Routes>
        <Route path='/' element={<Calendar />} />
        <Route path='/write' element={<WritingDiaryPage />} />
        <Route path='/complete' element={<DrawCompletedPage />} />
        <Route path='/selectdrawer' element={<SelectDrawerPage />} />
        <Route path='/drawerlist' element={<DrawerListPage />} />
        <Route path='write' element={<WritingDiaryPage />} />
        <Route path='thumbnail' element={<AlbumThumbnailPage />} />
        <Route path='list' element={<AlbumListPage />} />
        <Route path='emotion' element={<SelectEmotionPage />} />
      </Routes>
    </>
  );
}
export default App;
